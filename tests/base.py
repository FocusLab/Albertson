from datetime import datetime
import unittest

import boto
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

from mock import MagicMock, sentinel

from testconfig import config

from albertson import CounterPool

from .dynamodb_utils import dynamo_cleanup, DynamoDeleteMixin

ISO_FORMAT = '%Y-%m-%dT%H:%M:%S'


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

    def get_item(self, hash_key='test', attrs=None):
        table = self.get_table()
        real_attrs = {
            'count': 5,
            'created': '2012-01-02T23:32:13',
            'modified': '2012-01-02T24:33:23',
        }
        if attrs:
            real_attrs.update(attrs)

        item = table.new_item(
            hash_key=hash_key,
            attrs=real_attrs,
        )
        item.put()

        return item

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

    def test_create_table(self):
        pool = self.get_pool(table_name='albertson-create-test')

        table = pool.create_table()

        table.refresh(wait_for_active=True, retry_seconds=1)

        self.assertEquals(table.status, 'ACTIVE')

        table.delete()

    @dynamo_cleanup
    def test_get_existing_table(self):
        self.get_table()
        pool = self.get_pool()

        assert pool.get_table()

    def test_get_missing_table_without_auto_create(self):
        pool = self.get_pool(table_name='nonexistent')

        with self.assertRaises(boto.exception.DynamoDBResponseError):
            pool.get_table()

    def test_get_missing_table_with_auto_create(self):
        pool = self.get_pool(auto_create_table=True, table_name='nonexistent')
        pool.create_table = MagicMock(name='create_table')

        expected = sentinel.table_return
        pool.create_table.return_value = expected

        result = pool.get_table()

        pool.create_table.assert_called_with()
        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_create_item(self):
        hash_key = 'test'
        table = self.get_table()
        pool = self.get_pool(auto_create_table=True)
        now = datetime.utcnow().replace(microsecond=0)

        expected = {
            'counter_name': hash_key,
            'count': 0,
        }
        result = pool.create_item(hash_key=hash_key)

        self.assertDictContainsSubset(expected, result)

        created_offset = datetime.strptime(result['created_on'], ISO_FORMAT) - now
        modified_offset = datetime.strptime(result['modified_on'], ISO_FORMAT) - now

        self.assertLess(created_offset.seconds, 2)
        self.assertGreaterEqual(created_offset.seconds, 0)
        self.assertLess(modified_offset.seconds, 2)
        self.assertGreaterEqual(modified_offset.seconds, 0)

        with self.assertRaises(DynamoDBKeyNotFoundError):
            table.get_item(hash_key=hash_key, consistent_read=True)

    @dynamo_cleanup
    def test_get_missing_item(self):
        hash_key = 'test'
        pool = self.get_pool(auto_create_table=True)
        pool.create_item = MagicMock(name='create_item')

        expected = sentinel.item_return
        pool.create_item.return_value = expected

        result = pool.get_item(hash_key)

        pool.create_item.assert_called_with(hash_key=hash_key, start=0)
        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_get_existing_item(self):
        hash_key = 'test'
        table = self.get_table()
        expected = table.new_item(
            hash_key=hash_key,
            attrs={
                'count': 5,
                'created': '2012-01-02T23:32:13',
                'modified': '2012-01-02T24:33:23',
            }
        )
        expected.put()
        pool = self.get_pool()

        result = pool.get_item(hash_key)

        self.assertEqual(expected, result)

    @dynamo_cleanup
    def test_table_caching(self):
        pool = self.get_pool()
        pool._table = sentinel.cached_table

        expected = sentinel.cached_table
        result = pool.get_table()

        self.assertEqual(expected, result)

    @dynamo_cleanup
    def test_get_counter(self):
        pool = self.get_pool()
        name = 'test'

        result = pool.get_counter(name)

        self.assertEqual(pool, result.pool)

    @dynamo_cleanup
    def test_counter_name(self):

        pool = self.get_pool()
        expected = 'test'

        result = pool.get_counter(expected).name

        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_counter_count(self):
        pool = self.get_pool()
        name = 'test'

        expected = 0
        result = pool.get_counter(name).count

        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_counter_created_on(self):
        pool = self.get_pool()
        name = 'test'
        counter = pool.get_counter(name)

        expected = counter.dynamo_item['created_on']
        result = counter.created_on.isoformat()

        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_counter_modified_on(self):
        pool = self.get_pool()
        name = 'test'
        counter = pool.get_counter(name)

        expected = counter.dynamo_item['modified_on']
        result = counter.modified_on.isoformat()

        self.assertEquals(expected, result)

    @dynamo_cleanup
    def test_counter_refresh(self):
        pool = self.get_pool()
        expected = self.get_item()
        counter = pool.get_counter(expected.hash_key)

        self.assertEqual(expected, counter.dynamo_item)

        expected.add_attribute('count', 3)
        ret_val = expected.save(return_values='UPDATED_NEW')
        expected.update(ret_val['Attributes'])

        counter.refresh()

        self.assertEqual(expected, counter.dynamo_item)

    @dynamo_cleanup
    def test_counter_increment(self):
        table = self.get_table()
        pool = self.get_pool()
        item = self.get_item()
        counter = pool.get_counter(item.hash_key)
        print item
        print counter.dynamo_item

        self.assertEqual(item, counter.dynamo_item)
        old_count = counter.count

        now = datetime.utcnow().replace(microsecond=0)
        counter.increment()

        expected_count = old_count + 1
        self.assertEqual(expected_count, counter.count)

        fetched_item = table.get_item(item.hash_key)
        modified_offset = datetime.strptime(fetched_item['modified_on'], ISO_FORMAT) - now
        self.assertEqual(expected_count, fetched_item['count'])
        self.assertEqual(fetched_item, counter.dynamo_item)
        self.assertLess(modified_offset.seconds, 2)
        self.assertGreaterEqual(modified_offset.seconds, 0)
