from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from collections import namedtuple
from enum import Enum
import itertools
import json
from six import string_types
from six.moves.urllib import parse
from sqlalchemy import create_engine
import pandas as pd
import requests
from sqlalchemy import text

from .exceptions import Error, NotSupportedError, ProgrammingError


from requests.auth import HTTPBasicAuth
from requests_ntlm import HttpNtlmAuth
import sqlparse
from influxdb_client import InfluxDBClient

logger = logging.getLogger(__name__)


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(host='localhost', port=8086, scheme='http',
            trusted_connection=False, token=None,
            path='',username='',password=',', org=None):
    """
    Constructor for creating a connection to the database.

        >>> conn = InfluxDBClient(url=f"http://{host}:{port}", token=token, org=org)

    """
    return Connection(host, port, scheme, path='', trusted_connection=trusted_connection, token=token, org=org)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error(f'{self.__class__.__name__} already closed')
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return f(self, *args, **kwargs)

    return g


MAX_COLS = 254  # python 3.6 namedtuple constraint


def get_description_from_row(row, res):
    """
    Return description from a single row.

    """
    ret = []
    axis_tuple = res.getAxisTuple(0)
    ret.append(
        (
            "label",  # name
            Type.STRING,  # type_code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            True,  # [null_ok]
        )
    )
    if len(row) and isinstance(row[0], list):
        row = row[0]

    for i in range(min(len(row), MAX_COLS - 1)):
        c = row[i]
        if len(axis_tuple) > i:
            axis = axis_tuple[i]
            t = get_type(c.get('FmtValue'))
            ret.append(
                (
                    axis.get('Caption'),  # name
                    t,  # type_code
                    None,  # [display_size]
                    None,  # [internal_size]
                    None,  # [precision]
                    None,  # [scale]
                    t == Type.STRING,  # [null_ok]
                )
            )
    return ret


from zeep import ns

nsmap = {
    "soap": ns.SOAP_11,
    "soap-env": ns.SOAP_ENV_11,
    "wsdl": ns.WSDL,
    "xsd": ns.XSD,
    "sql": "urn:schemas-microsoft-com:xml-sql"
}


def get_description_from_rowset(res):
    """
    Return description from a single row.

    """
    ret = []
    for i in res.keys():
        c = res[i]

        t = Type.STRING #get_type_from_schema(c.get('type'))
        ret.append(
            (
                c,  # name
                t,  # type_code
                None,  # [display_size]
                None,  # [internal_size]
                None,  # [precision]
                None,  # [scale]
                t == Type.STRING,  # [null_ok]
            )
        )
    return ret


def get_type(value):
    """Infer type from value."""
    if isinstance(value, string_types) or value is None:
        return Type.STRING
    elif isinstance(value, (int, float)):
        return Type.NUMBER
    elif isinstance(value, bool):
        return Type.BOOLEAN

    raise Error(f'Value of unknown type: {value}')


def get_type_from_schema(t):
    """Infer type from value.
    http://books.xmlschemata.org/relaxng/relax-CHP-19.html
    xsd:anyURI - URI (Uniform Resource Identifier)
    xsd:base64Binary - Binary content coded as "base64"
    xsd:boolean - Boolean (true or false)
    xsd:byte - Signed value of 8 bits
    xsd:date - Gregorian calendar date
    xsd:dateTime - Instant of time (Gregorian calendar)
    xsd:decimal - Decimal numbers
    xsd:double - IEEE 64-bit floating-point
    xsd:duration - Time durations
    xsd:ENTITIES - Whitespace-separated list of unparsed entity references
    xsd:ENTITY - Reference to an unparsed entity
    xsd:float - IEEE 32-bit floating-point
    xsd:gDay - Recurring period of time: monthly day
    xsd:gMonth - Recurring period of time: yearly month
    xsd:gMonthDay - Recurring period of time: yearly day
    xsd:gYear - Period of one year
    xsd:gYearMonth - Period of one month
    xsd:hexBinary - Binary contents coded in hexadecimal
    xsd:ID - Definition of unique identifiers
    xsd:IDREF - Definition of references to unique identifiers
    xsd:IDREFS - Definition of lists of references to unique identifiers
    xsd:int - 32-bit signed integers
    xsd:integer - Signed integers of arbitrary length
    xsd:language - RFC 1766 language codes
    xsd:long - 64-bit signed integers
    xsd:Name - XML 1.O name
    xsd:NCName - Unqualified names
    xsd:negativeInteger - Strictly negative integers of arbitrary length
    xsd:NMTOKEN - XML 1.0 name token (NMTOKEN)
    xsd:NMTOKENS - List of XML 1.0 name tokens (NMTOKEN)
    xsd:nonNegativeInteger - Integers of arbitrary length positive or equal to zero
    xsd:nonPositiveInteger - Integers of arbitrary length negative or equal to zero
    xsd:normalizedString - Whitespace-replaced strings
    xsd:NOTATION - Emulation of the XML 1.0 feature
    xsd:positiveInteger - Strictly positive integers of arbitrary length
    xsd:QName - Namespaces in XML-qualified names
    xsd:short - 32-bit signed integers
    xsd:string - Any string
    xsd:time - Point in time recurring each day
    xsd:token - Whitespace-replaced and collapsed strings
    xsd:unsignedByte - Unsigned value of 8 bits
    xsd:unsignedInt - Unsigned integer of 32 bits
    xsd:unsignedLong - Unsigned integer of 64 bits
    xsd:unsignedShort - Unsigned integer of 16 bits
    """

    if t == "xsd:int" \
            or t == "xsd:long" \
            or t == "xsd:double" \
            or t == "xsd:float " \
            or t == "xsd:decimal " \
            or t == "xsd:unsignedByte" \
            or t == "xsd:unsignedInt" \
            or t == "xsd:unsignedLong" \
            or t == "xsd:unsignedShort" \
            or t == "xsd:short" \
            or t == "xsd:positiveInteger" \
            or t == "xsd:nonPositiveInteger" \
            or t == "xsd:nonNegativeInteger" \
            or t == "xsd:negativeInteger" \
            or t == "xsd:integer" \
            or t == "xsd:gYear" \
            or t == "xsd:gYearMonth" \
            or t == "xsd:gMonthDay" \
            or t == "xsd:gMonth" \
            or t == "xsd:gDay" \
            or t == "xsd:byte" \
            or t == "xsd:gMonthDay" \
            or t == "xsd:gMonthDay" \
            or t == "xsd:gMonthDay" \
            or t == "xsd:gMonthDay" \
            or t == "xsd:gMonthDay ":
        return Type.NUMBER
    elif t == "xsd:boolean":
        return Type.BOOLEAN
    else:
        return Type.STRING


class Connection(object):
    """Connection to a influxdb2 database."""

    def __init__(
            self,
            host='localhost',
            port=8086,
            scheme='http',
            path='',
            trusted_connection=False,
            token=None,
            org=None
    ):
        netloc = f'{host}:{port}'
        self.url = parse.urlunparse(
            (scheme, netloc, "", None, None, None))
        self.closed = False
        self.cursors = []
        self.org = org
        auth = None
        # if trusted_connection and username:
        #     auth = HttpNtlmAuth(username, password)
        # elif username:
        #     auth = HTTPBasicAuth(username, password)

        self.influxDb2 = InfluxDBClient(url=self.url, token=token, org=org)


    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):
    """Connection cursor."""

    def __init__(self, connection):
        self.url = connection.url
        self.connection = connection

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to an iterator after a successfull query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self):
        # consume the iterator
        results = list(self._results)
        n = len(results)
        self._results = iter(results)
        return n

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    def is_supported_query(self, parsed):
        if hasattr(parsed, "tokens"):
            for token in parsed.tokens:
                if "on" == token.value.lower():
                    return True
        return False

    def get_supported_query(self, parsed, original_parsed=None):
        if not original_parsed:
            original_parsed = parsed
        # query is not supported. Try finding a supported inner query
        if parsed and hasattr(parsed, "tokens"):
            for token in parsed.tokens:
                value = token.value
                if "from%%%bucket:" in value.replace("(", "%%%").replace(" ", "").lower():
                    # if self.is_supported_query(token):
                    # found supported query
                    # return it and modified
                    return (value.replace(')as qry',"").strip(' ()\'\"') if value.strip(' ').startswith("(") else value,
                            original_parsed.value.replace(value, "(SELECT * FROM Model)"))
                else:
                    ret = self.get_supported_query(token, original_parsed)
                    if ret:
                        return ret
                # else:
                #     ret = self.get_supported_query(token, original_parsed)
                #     if ret:
                #         return ret
        return None

    def execute_one_influxdb2(self, operation, schema):
        # `_stream_query` returns a generator that produces the rows; we need
        # to consume the first row so that `description` is properly set, so
        # let's consume it and insert it back.
        results = self._stream_query(operation, schema)
        first_row = next(results)
        self._results = itertools.chain([first_row], results)

        return self._results

    @check_closed
    def execute(self, operation, parameters=None, schema=None, **kwargs):
        operation = apply_parameters(operation, parameters or {})
        # `_stream_query` returns a generator that produces the rows; we need
        # to consume the first row so that `description` is properly set, so
        # let's consume it and insert it back.
        from_sqlite = self._stream_query_sqlite(operation, schema)
        if not from_sqlite:
            results = self.execute_one_influxdb2(operation, schema)
        else:
            results = self.from_sqlite_engine()

        first_row = next(results)
        self._results = itertools.chain([first_row], results)

        return self

    def from_sqlite_engine(self):
        with self.sqliteengine.connect() as connection:
            results = connection.execute(text(self.query_to_execute_on_db))
            self.description = results.cursor.description
            for row in results:
                yield row

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self.next()
        except StopIteration:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        return list(itertools.islice(self, size))

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self)

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return self

    @check_closed
    def __next__(self):
        return next(self._results)

    next = __next__

    def _stream_query(self, query, schema):
        """
        Stream rows from a query.

        This method will yield rows as the data is returned in chunks from the
        server.
        """
        self.description = None
        if query:
            query_api = self.connection.influxDb2.query_api()
            res = query_api.query_stream( query=query, org=self.connection.org )
            for record in res:
                Row = None
                row = record.values
                # update description
                if self.description is None:
                    self.description = get_description_from_rowset(record.values)

                # return row in namedtuple
                if Row is None:
                    keys = row.keys()
                    Row = namedtuple('Row', keys, rename=True)

                yield Row(*row)
            if hasattr(res, "getSlice"):
                xmla_slice = res.getSlice()

                # raise any error messages
                # if res.status_code != 200:
                #     payload = res.json()
                #     msg = (
                #         f'{payload["error"]} ({payload["errorClass"]}): ' +
                #         f'{payload["errorMessage"]}'
                #     )
                #     raise ProgrammingError(msg)

                Row = None
                axis_tuple = res.getAxisTuple(0)
                axis_tuple_1 = res.getAxisTuple(1)
                len_xmla_slice = len(xmla_slice)
                if len_xmla_slice > 0:
                    for irow in range(len_xmla_slice):
                        row = xmla_slice[irow]
                        if len(row) and isinstance(row[0], list):
                            row = row[0]
                        if len(axis_tuple_1) > irow:
                            first_col = axis_tuple_1[irow].get('Caption')
                        else:
                            first_col = None
                        # update description
                        if self.description is None:
                            self.description = get_description_from_row(row, res)

                        # return row in namedtuple
                        if Row is None:
                            keys = [d[0] for d in self.description]

                            Row = namedtuple('Row', keys, rename=True)
                        values = []
                        values.append(first_col)

                        for i in range(min(len(row), MAX_COLS - 1)):
                            if len(axis_tuple) > i:
                                c = row[i]
                                t = c.get('FmtValue')
                                values.append(t)
                        yield Row(*values)
            else:
                Row = None
                for irow in range(len([x for x in res])):
                    row = res.rows[irow]
                    # update description
                    if self.description is None:
                        self.description = get_description_from_rowset(res.description)

                    # return row in namedtuple
                    if Row is None:
                        keys = [d[0] for d in self.description]
                        Row = namedtuple('Row', keys, rename=True)

                    yield Row(*row)
        else:
            Row = namedtuple('Row', ['All'], rename=True)

            yield Row(*['1'])

    def _stream_query_sqlite(self, query, schema):
        """
        Stream rows from a query.

        This method will yield rows as the data is returned in chunks from the
        server.
        """
        if query:
            statements = sqlparse.split(query)
            if len(statements) > 1:
                logger.warning("Multiple queries not supported")
            statement = statements[0]
            parsed = sqlparse.parse(statement)[0]
            # check if we have a containing unsupported select
            if not self.is_supported_query(parsed):
                queries = self.get_supported_query(parsed)
                if queries:
                    self.query_to_execute_on_influxdb2 = queries[0]
                    self.query_to_execute_on_db = queries[1]
                    results = self.execute_one_influxdb2(self.query_to_execute_on_influxdb2, schema)
                    self.sqliteengine = create_engine('sqlite:///:memory:', echo=True)
                    with self.sqliteengine.connect() as connection:
                        connection.execute(text("drop table if exists model"))
                    df = pd.DataFrame(results)
                    df.to_sql('Model', self.sqliteengine.engine)
                    return True
        return False


def apply_parameters(operation, parameters):
    escaped_parameters = {
        key: escape(value) for key, value in parameters.items()
    }
    return operation % escaped_parameters


def escape(value):
    if value == '*':
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (list, tuple)):
        return ', '.join(escape(element) for element in value)
