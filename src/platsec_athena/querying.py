class Statement :
    def __init__(self,config):
        self.config = config
        self.output = "s3://"+self.config.output+"/DailyAthenaLogs/CloudTrail/"+self.config.eventstamp

