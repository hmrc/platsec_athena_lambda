class Partition :
    def __init__(self,config):
        self._config = config

    def get_query(self):
        partitions = self._config.get_partitions()
        locations = self._config.get_locations()
        partition_query = f'ALTER TABLE {self._config.table} ADD IF NOT EXISTS {partitions[0]} {locations[0]} {partitions[1]} {locations[1]}'
        return partition_query
