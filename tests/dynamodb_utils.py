import boto

from nose.tools import make_decorator

from testconfig import config


def dynamo_cleanup_func(extra_tables=None):
    conn = boto.connect_dynamodb(
        aws_access_key_id=config['aws']['access_key'],
        aws_secret_access_key=config['aws']['secret_key'],
    )
    tables = [config['albertson']['test_table_name']]

    if extra_tables:
        tables.extend(extra_tables)

    for table_name in tables:
        try:
            table = conn.get_table(table_name)
        except boto.exception.DynamoDBResponseError:
            table = None

        if table:
            items = table.scan()

            for item in items:
                item.delete()


def dynamo_cleanup(func, extra_tables=None):

    def new(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            dynamo_cleanup_func(extra_tables)
            raise

        dynamo_cleanup_func(extra_tables)

    new = make_decorator(func)(new)

    return new

