import unittest

import boto

from testconfig import config

from albertson.base import Counter

from .dynamodb_utils import dynamo_cleanup


class BaseCounterTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.tables = {}

        super(BaseCounterTests, self).__init__(*args, **kwargs)

    def get_connection(self):
        conn = getattr(self, '_conn', None)

        if not conn:
            conn = boto.connect_dynamodb(
                aws_access_key_id=config['aws']['access_key'],
                aws_secret_access_key=config['aws']['secret_key'],
            )
            self._conn = conn

        return conn

    def get_schema(self, **kwargs):
        conn = self.get_connection()
        real_kwargs = {
            'hash_key_name': 'counter_name',
            'hash_key_proto_value': 'S',
        }
        real_kwargs.update(kwargs)

        return conn.create_schema(**real_kwargs)

    def get_table(self, table_name=None, schema_kwargs=None):
        table_name = table_name or config['albertson']['test_table_name']
        table = self.tables.get(table_name, None)

        if table is None:
            conn = self.get_connection()

            try:
                table = conn.get_table(table_name)
            except boto.exception.DynamoDBResponseError:
                table = None

            if table is None:
                table = self.create_table(table_name, schema_kwargs)
                self.tables[table.name] = table

        return table

    def create_table(self, table_name=None, schema_kwargs=None):
        table_name = table_name or config['albertson']['test_table_name']
        schema_kwargs = schema_kwargs or {}

        conn = self.get_connection()
        schema = self.get_schema(**schema_kwargs)

        table = conn.create_table(
            name=table_name,
            schema=schema,
            read_units=3,
            write_units=5,
        )

        if table.status != 'ACTIVE':
            table.refresh(wait_for_active=True, retry_seconds=1)

        return table

    def get_counter(self, counter_class=None, **kwargs):
        counter_class = counter_class or Counter
        real_kwargs = {
            'aws_access_key': config['aws']['access_key'],
            'aws_secret_key': config['aws']['secret_key'],
            'table_name': config['albertson']['test_table_name'],
            'auto_create_table': False,
        }
        real_kwargs.update(kwargs)

        return counter_class(**real_kwargs)

    def test_base_init(self):
        counter = self.get_counter()

        counter.conn.list_tables()

    def test_get_init_table_name(self):
        counter = self.get_counter()

        expected = config['albertson']['test_table_name']
        result = counter.get_table_name()

        self.assertEquals(expected, result)

    def test_get_empty_table_name(self):
        counter = self.get_counter(table_name='')

        with self.assertRaises(NotImplementedError):
            counter.get_table_name()

    def test_get_attr_table_name(self):
        class TestCounter(Counter):
            table_name = 'some_name'

        counter = self.get_counter(counter_class=TestCounter, table_name=None)

        expected = 'some_name'
        result = counter.get_table_name()

        self.assertEquals(expected, result)

    def test_does_missing_table_exist(self):
        counter = self.get_counter(table_name='nonexistent')

        self.assertFalse(counter.does_table_exist())

    @dynamo_cleanup
    def test_does_existing_table_exist(self):
        self.get_table()
        counter = self.get_counter()

        assert counter.does_table_exist()
