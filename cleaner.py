import pandas as pd
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import sys

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

client = Client(host='10.0.0.1', port=30803, user='jaeger', password='ejhfehfhehf', database='jaeger')
current_time = datetime.now()
delete_query = f"DELETE FROM jopa WHERE timestamp = 1999"
client.execute(delete_query)
query_optimize = 'OPTIMIZE TABLE jopa FINAL'
client.execute(query_optimize)
client.disconnect()
