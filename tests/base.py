import unittest

from testconfig import config

from albertson.base import Counter


class BaseCounterTests(unittest.TestCase):

    def get_counter(self, counter_class=None, **kwargs):
        counter_class = counter_class or Counter
        real_kwargs = {
            'aws_access_key': config['aws']['access_key'],
            'aws_secret_key': config['aws']['secret_key'],
            'table_name': 'albertson-test',
        }
        real_kwargs.update(kwargs)

        return counter_class(**real_kwargs)

    def test_base_init(self):
        counter = self.get_counter()

        counter.conn.list_tables()

    def test_get_init_table_name(self):
        counter = self.get_counter()

        expected = 'albertson-test'
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
