import pandas as pd
from clickhouse_driver import Client
from datetime import datetime, timedelta
client = Client(host='172.17.32.21', port=9000, user='jaeger', password='utl8cb', database='jaeger')
current_time = datetime.now()
delete_query = f"DELETE FROM jaeger_spans_local WHERE timestamp < '{current_time.strftime('%Y-%m-%d %H:%M:%S')}'"
client.execute(delete_query)
query_optimize = 'OPTIMIZE TABLE jaeger_spans_local FINAL'
client.execute(query_optimize)
client.disconnect()
