from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types,util

import influxdb2_dbapi as db
from influxdb2_dbapi import exceptions
from influxdb_client import OrganizationsService

RESERVED_SCHEMAS = ['INFORMATION_SCHEMA']
"""
https://docs.microsoft.com/en-us/openspecs/sql_server_protocols/ms-ssas/aee32107-fde6-43bd-beeb-46e82851abf4

0 – (DBTYPE_EMPTY) Indicates that no value was specified.
2 – (DBTYPE_I2) Indicates a two-byte signed integer.
3 – (DBTYPE_I4) Indicates a four-byte signed integer.
4 – (DBTYPE_R4) Indicates a single-precision floating-point value.
5 – (DBTYPE_R8) Indicates a double-precision floating-point value.
6 – (DBTYPE_CY) Indicates a currency value. Currency is a fixed-point number with four digits to the right of the decimal point and is stored in an eight-byte signed integer scaled by 10,000.
7 – (DBTYPE_DATE) Indicates a date value. Date values are stored as Double, the whole part of which is the number of days since December 30, 1899, and the fractional part of which is the fraction of a day.
8 – (DBTYPE_BSTR) A pointer to a BSTR, which is a null-terminated character string in which the string length is stored with the string.
9 – (DBTYPE_IDISPATCH) Indicates a pointer to an IDispatch interface on an OLE object.
10 – (DBTYPE_ERROR) Indicates a 32-bit error code.
11 – (DBTYPE_BOOL) Indicates a Boolean value.
12 – (DBTYPE_VARIANT) Indicates an Automation variant.
13 – (DBTYPE_IUNKNOWN) Indicates a pointer to an IUnknown interface on an OLE object.
14 – (DBTYPE_DECIMAL) Indicates an exact numeric value with a fixed precision and scale. The scale is between 0 and 28.
16 – (DBTYPE_I1) Indicates a one-byte signed integer.
17 – (DBTYPE_UI1) Indicates a one-byte unsigned integer.
18 – (DBTYPE_UI2) Indicates a two-byte unsigned integer.
19 – (DBTYPE_UI4) Indicates a four-byte unsigned integer.
20 – (DBTYPE_I8) Indicates an eight-byte signed integer.
21 – (DBTYPE_UI8) Indicates an eight-byte unsigned integer.
72 – (DBTYPE_GUID) Indicates a GUID.
128 – (DBTYPE_BYTES) Indicates a binary value.
129 – (DBTYPE_STR) Indicates a string value.
130 – (DBTYPE_WSTR) Indicates a null-terminated Unicode character string.
131 – (DBTYPE_NUMERIC) Indicates an exact numeric value with a fixed precision and scale. The scale is between 0 and 38.
132 – (DBTYPE_UDT) Indicates a user-defined variable.
133 – (DBTYPE_DBDATE) Indicates a date value (yyyymmdd).
134 – (DBTYPE_DBTIME) Indicates a time value (hhmmss).
135 – (DBTYPE_DBTIMESTAMP) Indicates a date-time stamp (yyyymmddhhmmss plus a fraction in billionths).
136 - (DBTYPE_HCHAPTER) Indicates a four-byte chapter value used to identify rows in a child rowset.
"""

type_map = {
"0" : types.String,  #(DBTYPE_EMPTY) Indicates that no value was specified.
"2" : types.BigInteger,  #(DBTYPE_I2) Indicates a two-byte signed integer.
"3" : types.BigInteger,  #(DBTYPE_I4) Indicates a four-byte signed integer.
"4" : types.Float,  #(DBTYPE_R4) Indicates a single-precision floating-point value.
"5" : types.Float,  #(DBTYPE_R8) Indicates a double-precision floating-point value.
"6" : types.Float,  #(DBTYPE_CY) Indicates a currency value. Currency is a fixed-point number with four digits to the right of the decimal point and is stored in an eight-byte signed integer scaled by 10,000.
"7" : types.Float,  #(DBTYPE_DATE) Indicates a date value. Date values are stored as Double, the whole part of which is the number of days since December 30, 1899, and the fractional part of which is the fraction of a day.
"8" : types.String,  #(DBTYPE_BSTR) A pointer to a BSTR, which is a null-terminated character string in which the string length is stored with the string.
"9" : types.BigInteger,  #(DBTYPE_IDISPATCH) Indicates a pointer to an IDispatch interface on an OLE object.
"10" : types.BigInteger,  #(DBTYPE_ERROR) Indicates a 32-bit error code.
"11" : types.Boolean,  #(DBTYPE_BOOL) Indicates a Boolean value.
"12" : types.String,  #(DBTYPE_VARIANT) Indicates an Automation variant.
"13" : types.String,  #(DBTYPE_IUNKNOWN) Indicates a pointer to an IUnknown interface on an OLE object.
"14" : types.Float,  #(DBTYPE_DECIMAL) Indicates an exact numeric value with a fixed precision and scale. The scale is between 0 and 28.
"16" : types.BigInteger,  #(DBTYPE_I1) Indicates a one-byte signed integer.
"17" : types.BigInteger,  #(DBTYPE_UI1) Indicates a one-byte unsigned integer.
"18" : types.BigInteger,  #(DBTYPE_UI2) Indicates a two-byte unsigned integer.
"19" : types.BigInteger,  #(DBTYPE_UI4) Indicates a four-byte unsigned integer.
"20" : types.BigInteger,  #(DBTYPE_I8) Indicates an eight-byte signed integer.
"21" : types.BigInteger,  #(DBTYPE_UI8) Indicates an eight-byte unsigned integer.
"72" : types.String,  #(DBTYPE_GUID) Indicates a GUID.
"128" : types.String,  #(DBTYPE_BYTES) Indicates a binary value.
"129" : types.String,  #(DBTYPE_STR) Indicates a string value.
"130" : types.String,  #(DBTYPE_WSTR) Indicates a null-terminated Unicode character string.
"131" : types.Float,  #(DBTYPE_NUMERIC) Indicates an exact numeric value with a fixed precision and scale. The scale is between 0 and 38.
"132" : types.String,  #(DBTYPE_UDT) Indicates a user-defined variable.
"133" : types.String,  #(DBTYPE_DBDATE) Indicates a date value (yyyymmdd).
"134" : types.String,  #(DBTYPE_DBTIME) Indicates a time value (hhmmss).
"135" : types.String,  #(DBTYPE_DBTIMESTAMP) Indicates a date-time stamp (yyyymmddhhmmss plus a fraction in billionths).
"136" : types.String,  # (DBTYPE_HCHAPTER) Indicates a four-byte chapter value used to identify rows in a child rowset.

}
# type_map = {
#     'char': types.String,
#     'varchar': types.String,
#     'float': types.Float,
#     'decimal': types.Float,
#     'real': types.Float,
#     'double': types.Float,
#     'boolean': types.Boolean,
#     'tinyint': types.BigInteger,
#     'smallint': types.BigInteger,
#     'integer': types.BigInteger,
#     'bigint': types.BigInteger,
#     'timestamp': types.TIMESTAMP,
#     'date': types.DATE,
# }


class UniversalSet(object):
    def __contains__(self, item):
        return True


class Influxdb2IdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class Influxdb2Compiler(compiler.SQLCompiler):
    pass


class Influxdb2TypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type DATETIME is not supported')

    def visit_TIME(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type TIME is not supported')

    def visit_BINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type BINARY is not supported')

    def visit_VARBINARY(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type VARBINARY is not supported')

    def visit_BLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type BLOB is not supported')

    def visit_CLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type CBLOB is not supported')

    def visit_NCLOB(self, type_, **kwargs):
        raise exceptions.NotSupportedError('Type NCBLOB is not supported')


class Influxdb2Dialect(default.DefaultDialect):

    name = 'influxdb2'
    scheme = 'http'
    driver = 'rest'
    preparer = Influxdb2IdentifierPreparer
    statement_compiler = Influxdb2Compiler
    type_compiler = Influxdb2TypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    _has_events = True

    @classmethod
    def dbapi(cls):
        return db

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 80,
            'path': url.database,
            'token': url.query['token'],
            'org': url.query['org'],
            'scheme': self.scheme,
            'username': url.username,
            'password': url.password,
            'trusted_connection': url.query.get("trusted_connection") == "yes"
        }
        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        # Each Influxdb2 datasource appears as a table in the "Influxdb2" schema. This
        # is also the default schema, so Influxdb2 datasources can be referenced as
        # either Influxdb2.dataSourceName or simply dataSourceName.
        # result = connection.raw_connection().connection.influxDb2.getDBSchemaCatalogs()
        #
        # return [
        #     row.CATALOG_NAME for row in result
        #     if row.CATALOG_NAME not in RESERVED_SCHEMAS
        # ]
        # organizations_service = OrganizationsService(api_client=connection.connection.influxDb2.api_client)
        # organizations = organizations_service.get_orgs()
        # for organization in organizations.orgs:
        #     print(f'name: {organization.name}, id: {organization.id}')
        # return organizations.orgs
        #
        # return [
        #     organization.name for organization in organizations.orgs
        #     if organization.name not in RESERVED_SCHEMAS
        # ]

        curs = connection.connection.cursor()
        curs.execute(f""" 
                    import "influxdata/influxdb/schema"
                    buckets() 
                """)
        return [row.name  for row in curs ]

    def has_table(self, connection, table_name, schema=None):
        """TODO"""
        return True
        # if schema:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaDimensions(properties={"Catalog":schema})
        # else:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaDimensions()
        # return len([
        #     row.DIMENSION_NAME for row in result if row.DIMENSION_NAME == table_name
        # ])>0


    def get_table_names(self, connection, schema=None, **kwargs):
        # if schema:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaDimensions(properties={"Catalog":schema})
        # else:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaDimensions()
        # return [  row.DIMENSION_NAME for row in result   ]
        curs = connection.connection.cursor()
        curs.execute(f""" 
                            import "influxdata/influxdb/schema"
                            schema.measurements(bucket: "collectd") 
                        """)
        return [row.value  for row in curs ]
    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        curs = connection.connection.cursor()
        curs.execute(f"""
            SELECT  * 
            FROM (

             from(bucket: "{schema}")
              |> range(start: -1m, stop: 1m)
              |> filter(fn: (r) => r["_measurement"] == "{table_name}")
              |> limit(n: 1)

            )as qry
             LIMIT 10
        """)
        for row in curs:
            return [
                {
                    'name': field,
                    'type': types.String,
                    'nullable': True,
                    'default': None  # get_default(row.COLUMN_DEFAULT),
                }
                for field in row._fields
            ]
        return {}
        # if schema:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaLevels(properties={f"Catalog":f"{schema}"})
        # else:
        #     result = connection.raw_connection().connection.influxDb2.getMDSchemaLevels(properties={f"Catalog":f"{schema}"})
        # if table_name:
        #     result =  [row for row in result if row.get("DIMENSION_UNIQUE_NAME").strip("[]") == table_name]
        #
        # return [
        #     {
        #         'name': row.get("LEVEL_UNIQUE_NAME"),
        #         'type': type_map[row.get("LEVEL_DBTYPE")],
        #         'nullable': False,
        #         'default': None #get_default(row.COLUMN_DEFAULT),
        #     }
        #     for row in result
        # ]

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {'text': ''}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kwargs
    ):
        return []

    def get_view_definition(
        self,
        connection,
        view_name,
        schema=None,
        **kwargs
    ):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    @util.memoized_property
    def _dialect_specific_select_one(self):
        return ""

Influxdb2HTTPDialect = Influxdb2Dialect


class Influxdb2HTTPSDialect(Influxdb2Dialect):

    scheme = 'https'


def get_is_nullable(Influxdb2_is_nullable):
    # this should be 'YES' or 'NO'; we default to no
    return Influxdb2_is_nullable.lower() == 'yes'


def get_default(Influxdb2_column_default):
    # currently unused, returns ''
    return str(Influxdb2_column_default) if Influxdb2_column_default != '' else None
