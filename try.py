import clickhouse_connect

import clickhouse_connect

ch = clickhouse_connect.get_client(
    host='b45p3y549w.ap-south-1.aws.clickhouse.cloud',
    user='default',
    password='VPJUt8.YQVLeL',
    secure=True
)

print(ch.query("SELECT version()").result_rows)