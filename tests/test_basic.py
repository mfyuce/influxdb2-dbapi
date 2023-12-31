# -*- coding: utf-8 -*-

from .context import influxdb2_dbapi

import unittest


class BasicTestSuite(unittest.TestCase):

    def test_rows_from_chunks_empty(self):
        chunks = []
        expected = []
        result = list(influxdb2_dbapi.db.rows_from_chunks(chunks))
        self.assertEquals(result, expected)

    def test_rows_from_chunks_single_chunk(self):
        chunks = ['[{"name": "alice"}, {"name": "bob"}, {"name": "charlie"}]']
        expected = [
            {'name': 'alice'},
            {'name': 'bob'},
            {'name': 'charlie'},
        ]
        result = list(influxdb2_dbapi.db.rows_from_chunks(chunks))
        self.assertEquals(result, expected)

    def test_rows_from_chunks_multiple_chunks(self):
        chunks = [
            '[{"name": "alice"}, {"name": "b',
            'ob"}, {"name": "charlie"}]',
        ]
        expected = [
            {'name': 'alice'},
            {'name': 'bob'},
            {'name': 'charlie'},
        ]
        result = list(influxdb2_dbapi.db.rows_from_chunks(chunks))
        self.assertEquals(result, expected)

    def test_rows_from_chunks_bracket_in_string(self):
        chunks = ['[{"name": "ali{ce"}, {"name": "bob"}]']
        expected = [
            {'name': 'ali{ce'},
            {'name': 'bob'},
        ]
        result = list(influxdb2_dbapi.db.rows_from_chunks(chunks))
        self.assertEquals(result, expected)

    def test_rows_from_chunks_quote_in_string(self):
        chunks = [r'[{"name": "ali\"ce"}, {"name": "bob"}]']
        expected = [
            {'name': 'ali"ce'},
            {'name': 'bob'},
        ]
        result = list(influxdb2_dbapi.db.rows_from_chunks(chunks))
        self.assertEquals(result, expected)


if __name__ == '__main__':
    unittest.main()
