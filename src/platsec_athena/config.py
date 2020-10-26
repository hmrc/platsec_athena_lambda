class LambdaEnvironment :
    def __init__(self,db, table, bucket, output, account,eventstamp,regions
            ):
        self._db = db
        self._table = table
        self._bucket = bucket
        self._output = output
        self._account = account
        self._eventstamp = eventstamp
        self._athena_year = str(eventstamp[:4])
        self._athena_month = str(eventstamp[5:7])
        self._athena_day = str(eventstamp[8:10])
        self._regions = regions
    @property
    def output(self):
        return self._output

    @property
    def eventstamp(self):
        return self._eventstamp

    @property
    def athena_year(self):
        return self._athena_year

    @property
    def athena_month(self):
        return self._athena_month

    @property
    def athena_day(self):
        return self._athena_day

    @property
    def db(self):
        return self._db

    @property
    def table(self):
        return self._table

    @property
    def regions(self):
        return self._regions

    @regions.setter
    def regions(self,regions):
        if not len(regions) > 0:
            raise IndexError("A region must be specified")
        else:
            self._regions = regions

    def get_querydates(self):
        query_results={}
        query_date=f'year=\'{self._athena_year}\' AND month=\'{self._athena_month}\' AND day=\'{self._athena_day}\';'
        source_clause =f'{self._db}.{self.table}'

        query_results["date_clause"]=query_date
        query_results["source_clause"]=source_clause
        return query_results

    def get_partitions(self):
        if len(self._regions) > 0 :
            return list(map(lambda a:f'partition (region="{a}", month="{self._athena_month}", day="{self._athena_day}", year="{self.athena_year}")',self._regions))

