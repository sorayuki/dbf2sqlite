#include <string>
#include <exception>
#include <sstream>
#include <functional>
#include <iostream>
#include <vector>

#include "dbase/dbf.h"
#include "sqlite3/sqlite3.h"

using FieldListT = std::vector<DBF_FIELD_INFO>;

namespace {
    class Releaser
    {
        std::function<void()> func_;
    public:
        Releaser(std::function<void()> x) : func_(x) {}
        ~Releaser() { func_(); }
    };
};

static int cpp_sqlite3_exec(sqlite3* db, const char* sqlcommand, int(*cb)(void*,int,char**,char**), void* userdata, std::string& errmsg)
{
    char* szerrmsg = 0;
    int ret = sqlite3_exec(db, sqlcommand, cb, userdata, &szerrmsg);
    if (szerrmsg)
    {
        errmsg = szerrmsg;
        sqlite3_free(szerrmsg);
    }
    return ret;
}

static FieldListT GetDBFFields(DBF_HANDLE dbf)
{
    FieldListT result;
    dbf_uint fieldCount = dbf_getfieldcount(dbf);
    for (dbf_uint fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex)
    {
        DBF_FIELD_INFO field = {};
        if (!dbf_getfield_info(dbf, fieldIndex, &field))
            throw std::runtime_error(std::string("fail to get field ") + std::to_string(fieldIndex));
        result.emplace_back(field);
    }
    return result;
}

static void SQLCreateTable(sqlite3* sql, const char* tablename, const FieldListT& fields)
{
    std::stringstream sqlcommand;
    sqlcommand << "create table \"" << tablename << "\" (recordid integer, ";

    for (size_t i = 0; i < fields.size(); ++i)
    {
        auto& field = fields[i];
        if (i != 0) sqlcommand << ", ";
        sqlcommand << field.name << ' ';
        switch (field.type)
        {
        case DBF_DATA_TYPE_CHAR:
            sqlcommand << "varchar(" << field.length << ')';
            break;
        case DBF_DATA_TYPE_INTEGER:
            if (field.decimals == 0)
                sqlcommand << "integer";
            else
                sqlcommand << "real";
            break;
        case DBF_DATA_TYPE_FLOAT:
            sqlcommand << "real";
            break;
        case DBF_DATA_TYPE_BOOLEAN:
            sqlcommand << "boolean";
            break;
        default:
            throw std::runtime_error(std::string("dbf2sql: not supported field ") + field.name);
            break;
        }
        sqlcommand << ' ';
    }
    sqlcommand << ")";

    std::string sqlerrmsg;
    int ret = cpp_sqlite3_exec(sql, sqlcommand.str().c_str(), nullptr, nullptr, sqlerrmsg);
    if (ret != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite3 error during create table: " + sqlerrmsg) + ": " + sqlite3_errmsg(sql));

    sqlcommand = std::stringstream();
    sqlcommand << "create index " << tablename << "_recordid_index on " << tablename << "(recordid)";
    ret = cpp_sqlite3_exec(sql, sqlcommand.str().c_str(), nullptr, nullptr, sqlerrmsg);
    if (ret != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite3 error during create index: " + sqlerrmsg) + ": " + sqlite3_errmsg(sql));
}

static void DBDataToSQL(DBF_HANDLE db, const FieldListT& fields, sqlite3* sql, const char* tablename)
{
    sqlite3_stmt* stmt = 0;

    std::vector<std::function<void(int f)> > bindFunctions;
    std::vector<std::string> buf(fields.size());
    std::stringstream sqlcommand;
    int currecord = 0;
    sqlcommand << "insert into " << tablename << "(";
    for (size_t i = 0; i < fields.size(); ++i)
    {
        auto& field = fields[i];
        if (i > 0) sqlcommand << ", ";
        sqlcommand << field.name;

        switch (field.type)
        {
        case DBF_DATA_TYPE_CHAR:
            bindFunctions.emplace_back([&](int f){
                buf[f].resize(fields[f].length);
                auto fielddata = dbf_getfieldptr(db, f);
                size_t fsize = dbf_getfield(db, fielddata, &buf[f][0], fields[f].length, DBF_DATA_TYPE_CHAR);
                sqlite3_bind_text(stmt, f + 1, buf[f].data(), fsize, nullptr);
            });
            break;
        case DBF_DATA_TYPE_INTEGER:
            bindFunctions.emplace_back([&](int f){
                auto fielddata = dbf_getfieldptr(db, f);
                if (fields[f].decimals == 0)
                {
                    long data;
                    if (!dbf_getfield_numeric(db, fielddata, &data))
                        throw std::runtime_error(std::string("dbf: fail to read integer field ") + fields[f].name + " for record " + std::to_string(currecord));
                    sqlite3_bind_int(stmt, f + 1, data);
                }
                else
                {
                    double data;
                    if (!dbf_getfield_float(db, fielddata, &data))
                        throw std::runtime_error(std::string("dbf: fail to read float field ") + fields[f].name + " for record " + std::to_string(currecord));
                    sqlite3_bind_double(stmt, f + 1, data);
                }
            });
            break;
        case DBF_DATA_TYPE_FLOAT:
            bindFunctions.emplace_back([&](int f) {
                auto fielddata = dbf_getfieldptr(db, f);
                double data;
                if (!dbf_getfield_float(db, fielddata, &data))
                    throw std::runtime_error(std::string("dbf: fail to read float field ") + fields[f].name + " for record " + std::to_string(currecord));
                sqlite3_bind_double(stmt, f + 1, data);
            });
            break;
        case DBF_DATA_TYPE_BOOLEAN:
            bindFunctions.emplace_back([&](int f) {
                auto fielddata = dbf_getfieldptr(db, f);
                BOOL data;
                if (!dbf_getfield_bool(db, fielddata, &data))
                    throw std::runtime_error(std::string("dbf: fail to read bool field ") + fields[f].name + " for record " + std::to_string(currecord));
                sqlite3_bind_int(stmt, f + 1, data);
            });
            break;
        default:
            throw std::runtime_error("not implement");
        }
    }
    sqlcommand << ", recordid) values (";
    for (size_t i = 0; i < fields.size() + 1; ++i)
        sqlcommand << (i == 0 ? "" : ",") << "?";
    sqlcommand << ")";

    if (sqlite3_prepare_v2(sql, sqlcommand.str().c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        throw std::runtime_error(std::string("sqlite: fail to parse sql ") + sqlcommand.str() + ": " + sqlite3_errmsg(sql));
    }
    Releaser stmtReleaser([&](){
        if (stmt) sqlite3_finalize(stmt);
    });

    dbf_uint recordCount = dbf_getrecordcount(db);
    for (dbf_uint i = 0; i < recordCount; ++i)
    {
        dbf_setposition(db, i);
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        for (size_t f = 0; f < fields.size(); ++f)
            bindFunctions[f](f);
        sqlite3_bind_int(stmt, fields.size() + 1, i + 1);
        if (sqlite3_step(stmt) != SQLITE_DONE)
            throw std::runtime_error(std::string("sqlite: fail to insert data for record ") + std::to_string(i) + ": " + sqlite3_errmsg(sql));
    }
}

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        std::cerr << "usage: " << "dbf2sqlite [dbf file] [sqlite3 file] [table name]" << std::endl;
        std::cerr << "since dbf file has record number, it converted to recordid column." << std::endl;
        return 1;
    }
    try
    {
        const char* dbffile = argv[1];
        const char* sqlitefile = argv[2];
        const char* tablename = argv[3];

        DBF_OPEN dbfopeninfo = {};
        dbfopeninfo.charconv = dbf_charconv_off;
        dbfopeninfo.editmode = dbf_editmode_readonly;
        dbfopeninfo.memo = false;
        DBF_HANDLE db = dbf_open(dbffile, &dbfopeninfo);
        if (db == 0)
            throw std::runtime_error("fail to open dbf file");
        Releaser dbReleaser([&](){
            dbf_close(&db);
        });


        sqlite3* sql;
        if (sqlite3_open(sqlitefile, &sql) != SQLITE_OK)
            throw std::runtime_error("fail to open sqlite file");
        Releaser sqlReleaser([&](){
            if (sql)
                sqlite3_close(sql);
        });


        auto fields = GetDBFFields(db);
        SQLCreateTable(sql, tablename, fields);
        sqlite3_exec(sql, "begin transaction", nullptr, nullptr, nullptr);
        DBDataToSQL(db, fields, sql, tablename);
        sqlite3_exec(sql, "commit", nullptr, nullptr, nullptr);
        sqlite3_close(sql);
    }
    catch (std::runtime_error e)
    {
        std::cerr << e.what();
    }
    return 0;
}
