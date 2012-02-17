import boto


class CounterPool(object):
    '''
    Handles schema level interactions with DynamoDB and generates individual
    counters as needed.
    '''
    table_name = None
    schema = {
        'hash_key_name': 'counter_name',
        'hash_key_proto_value': 'S',
    }
    read_units = 3
    write_units = 5

    def __init__(self, aws_access_key=None, aws_secret_key=None, table_name=None, schema=None, read_units=None, write_units=None, auto_create_table=True, ):
        self.conn = self.get_conn(aws_access_key, aws_secret_key)
        self.table_name = table_name or self.table_name
        self.schema = schema or self.schema
        self.read_units = read_units or self.read_units
        self.write_units = write_units or self.write_units
        self.auto_create_table = auto_create_table

        super(CounterPool, self).__init__()

    def get_conn(self, aws_access_key=None, aws_secret_key=None):
        return boto.connect_dynamodb(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

    def get_table_name(self):
        if not self.table_name:
            raise NotImplementedError(
                'You must provide a table_name value or override the get_table_name method'
            )
        return self.table_name

    def get_schema(self):
        if not self.schema:
            raise NotImplementedError(
                'You must provide a schema value or override the get_schema method'
            )

        return self.conn.create_schema(**self.schema)

    def get_read_units(self):
        return self.read_units

    def get_write_units(self):
        return self.write_units

    def create_table(self):
        return self.conn.create_table(
            name=self.get_table_name(),
            schema=self.get_schema(),
            read_units=self.get_read_units(),
            write_units=self.get_write_units(),
        )

    def get_table(self):
        try:
            table = self.conn.get_table(self.get_table_name())
        except boto.exception.DynamoDBResponseError:
            if self.auto_create_table:
                table = self.create_table()
            else:
                raise

        return table
