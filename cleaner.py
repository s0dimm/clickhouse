import pandas as pd
from clickhouse_driver import Client
from datetime import datetime, timedelta
client = Client(host='172.17.32.21', port=9000, user='jaeger', password='utl8cb', database='jaeger')
current_time = datetime.now()
threshold_time = current_time - timedelta(days=5)
delete_query = f"DELETE FROM jaeger_spans_local WHERE timestamp < '{threshold_time.strftime('%Y-%m-%d %H:%M:%S')}'"
client.execute(delete_query)
client.disconnect()
