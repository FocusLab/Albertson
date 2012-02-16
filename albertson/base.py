import boto


class Counter(object):
    '''
    Base counter class that can be used directly or overwritten as needed.
    '''

    def __init__(self, aws_access_key=None, aws_secret_key=None):
        self.conn = self.get_conn(aws_access_key, aws_secret_key)

    def get_conn(self, aws_access_key=None, aws_secret_key=None):
        return boto.connect_dynamodb(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
