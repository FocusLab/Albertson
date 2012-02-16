import unittest

from testconfig import config

from albertson.base import Counter


class BaseCounterTests(unittest.TestCase):

    def test_base_init(self):
        counter = Counter(
            aws_access_key=config['aws']['access_key'],
            aws_secret_key=config['aws']['secret_key'],
        )

        counter.conn.list_tables()
