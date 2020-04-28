# GENERAL
BATCH_SIZE = 97
TOTAL_TIME = 100
BROKER_URL = 'kafka://master.cluster2:9092'

# TOPIC
ACCUMULATION_TOPIC = 'accumulation'
AVAILABILITY_TOPIC = 'availability'
PERFORMANCE_TOPIC = 'performance'
QUALITY_TOPIC = 'quality'
OEE_TOPIC = 'oee'

# TABLE
GOOD_ACCUMULATED_TABLE = 'good_accumulated'
REJECT_ACCUMULATED_TABLE = 'reject_accumulated'
N_DOWNTIME_TABLE = 'n_downtime'
AVAILABILITY_TABLE = 'availability'
RUNTIME_TABLE = 'runtime'
PERFORMANCE_TABLE = 'performance'
QUALITY_TABLE = 'quality'
OEE_TABLE = 'oee'

# KEY
KEY = 'all'

# BROKERS
BOOTSTRAP_SERVERS = ['192.168.56.101',]
BROKER = 'kafka://master.cluster2:9092'


# N_MACHINES
N_MACHINES = 10