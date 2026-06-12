# kafkactl

First install kafkactl to test connectivity:

    wget https://github.com/deviceinsight/kafkactl/releases/download/v5.19.0/kafkactl_5.19.0_linux_amd64.deb
    sudo dpkg -i kafkactl_5.19.0_linux_amd64.deb

Copy and set up the config file with your credentials provided by DataFree:

    cp kafkactl.yml.example kafkactl.yml
    # Update with your details

Test connectivity:

    $ kafkactl -C kafkactl.yml get topics
    TOPIC             PARTITIONS     REPLICATION FACTOR
    test              5              0

You can also look at the data stream directly:

    $ kafkactl -C kafkactl.yml consume test --from-beginning
    2026-06-09T03:05:13.806Z,abc1234602838088,1234,1.2.3.5,za,free,200,Connection Established,4235,4767,https://google.com/foo,65501,,,47775,2026-06-09 04:00:33.107774

# Saving the feed into ClickHouse

Set up the kafka config and update it with your kafka credentials provided by DataFree:

    cp docker-compose/clickhouse/config.d/kafka.xml.example docker-compose/clickhouse/config.d/kafka.xml
    # Update with your details

Spin up the clickhouse instance:

    docker compose up -d
    docker compose exec clickhouse clickhouse-client -mn

Try running the following queries in clickhouse to show the data being populated:


```
e5b06d4aca17 :) select * from premium_reporting_raw limit 10;

SELECT *
FROM premium_reporting_raw
LIMIT 10

Query id: 5e401a20-f910-4b7c-be65-fda4b5bb5831

Row 1:
──────
request_timestamp:   2026-06-09 03:00:34.187
device_id:           12345687923409820938409823
app_id:              1234
ipv4_address:        1.2.3.4
country_code:        AE
payment_type:        paid
http_status_code:    200
http_status_msg:     
request_bytes:       3851
response_bytes:      1246
url:                 https://google.com/foo
hni:                 42403
connect_endpoint_ip: 0.0.0.0
client_uid:          
logproc_run_id:      47775
logproc_time:        2026-06-09 04:00:33.107

e5b06d4aca17 :) select toStartOfInterval(request_timestamp, interval 1 hour) as hour, sum(event_count), sum(total_request_bytes + total_response_bytes) from premium_reporting_rollup group by 1 order by 1;

Query id: 22d10ff4-8366-48c0-ab0b-aedec40a1817

   ┌────────────────hour─┬─sum(event_count)─┬─sum(plus(tot⋯nse_bytes))─┐
1. │ 2026-06-09 02:00:00 │           172723 │               2354259829 │ -- 2.35 billion
2. │ 2026-06-09 03:00:00 │          1770755 │              24747106456 │ -- 24.75 billion
3. │ 2026-06-09 04:00:00 │          3083563 │              41046819155 │ -- 41.05 billion
4. │ 2026-06-09 05:00:00 │          3817966 │              49383835810 │ -- 49.38 billion
5. │ 2026-06-09 06:00:00 │          4077643 │              54312631679 │ -- 54.31 billion
6. │ 2026-06-09 07:00:00 │          3603170 │              46430663669 │ -- 46.43 billion
7. │ 2026-06-09 08:00:00 │          2676617 │              33979404377 │ -- 33.98 billion
8. │ 2026-06-09 09:00:00 │          1218944 │              15840725814 │ -- 15.84 billion
9. │ 2026-06-09 10:00:00 │           829477 │              10525922599 │ -- 10.53 billion
```


# Kafka Line Format

You receive via Kafka a CSV with the following columns:

- Request timestamp
- Device ID
- App ID
- IP address
- country
- connection type: (paid, free (=zero-rated), wifi, unknown)
- http status code
- http status message
- request bytes
- response bytes
- request url
- HNI (MCC + MNC)
- Connect endpoint IP
- Client uid
- Log processing run ID by DataFree servers
- Log processing timestamp by DataFree servers

