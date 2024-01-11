from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import sys
import os

host = os.environ['HOST']
port = os.environ['PORT']
user = os.environ['USER']
password = os.environ['PASSWORD']
database = os.environ['DATABASE']
table = os.environ['TABLE']

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

client = Client((host), (port), (database), (user), (password))
current_time = datetime.now()
delete = f"DELETE FROM {table} WHERE timestamp < '{current_time.strftime('%Y-%m-%d')}'"
optimize = f"OPTIMIZE TABLE {table} FINAL"

def clear(client, delete, optimize):
    client.execute(delete)
    client.execute(optimize)
    client.disconnect()

if __name__ == "__main__":
    clear(client, delete, optimize)
