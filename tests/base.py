import unittest

import boto

from testconfig import config

from albertson.base import CounterPool

from .dynamodb_utils import dynamo_cleanup, DynamoDeleteMixin


class BaseCounterPoolTests(DynamoDeleteMixin, unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.tables = {}

        super(BaseCounterPoolTests, self).__init__(*args, **kwargs)

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

    def get_pool(self, pool_class=None, **kwargs):
        pool_class = pool_class or CounterPool
        real_kwargs = {
            'aws_access_key': config['aws']['access_key'],
            'aws_secret_key': config['aws']['secret_key'],
            'table_name': config['albertson']['test_table_name'],
            'auto_create_table': False,
        }
        real_kwargs.update(kwargs)

        return pool_class(**real_kwargs)

    def test_base_init(self):
        pool = self.get_pool()

        pool.conn.list_tables()

    def test_get_init_table_name(self):
        pool = self.get_pool()

        expected = config['albertson']['test_table_name']
        result = pool.get_table_name()

        self.assertEquals(expected, result)

    def test_get_empty_table_name(self):
        pool = self.get_pool(table_name='')

        with self.assertRaises(NotImplementedError):
            pool.get_table_name()

    def test_get_attr_table_name(self):
        class TestCounterPool(CounterPool):
            table_name = 'some_name'

        pool = self.get_pool(pool_class=TestCounterPool, table_name=None)

        expected = 'some_name'
        result = pool.get_table_name()

        self.assertEquals(expected, result)

    def test_get_default_schema(self):
        pool = self.get_pool()
        conn = self.get_connection()

        expected = conn.create_schema(**pool.schema).dict
        result = pool.get_schema().dict

        self.assertEquals(expected, result)
        self.assertIsNotNone(result)

    def test_get_init_schema(self):
        schema_dict = {'hash_key_name': 'test', 'hash_key_proto_value': 'S'}
        pool = self.get_pool(schema=schema_dict)
        conn = self.get_connection()

        expected = conn.create_schema(**schema_dict).dict
        result = pool.get_schema().dict

        self.assertEquals(expected, result)

    def test_get_empty_schema(self):
        pool = self.get_pool()
        pool.schema = None

        with self.assertRaises(NotImplementedError):
            pool.get_schema()

    def test_get_missing_table(self):
        pool = self.get_pool(table_name='nonexistent')

        with self.assertRaises(boto.exception.DynamoDBResponseError):
            pool.get_table()

    @dynamo_cleanup
    def test_does_existing_table_exist(self):
        self.get_table()
        pool = self.get_pool()

        assert pool.get_table()
