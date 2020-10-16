class LambdaEnvironment :
    def __init__(db, table, bucket, output, account):
        self.db = db
        self.table = table
        self.bucket = bucket
        self.output = output
        self.account = account
