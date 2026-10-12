# Kafka и ClickHouse

Нужно загрузить тестовый csv в kafka. Таблицы создаются из `analytics/schema.sql`, запрос лежит в `analytics/max_category_by_state.sql`. используется `AggregatingMergeTree` как оптимизация.

пререквизиты -  docker и docker-compose

```bash
docker-compose up --build -d
docker-compose ps -a
```

```bash
mkdir -p data
cp /путь/к/test.csv data/test.csv
docker-compose build csv_loader
docker-compose run --rm csv_loader /data/test.csv
```

```bash
docker-compose exec -T clickhouse clickhouse-client --user homework --password boba_aboba --query 'SELECT count() FROM transactions'
docker-compose exec -T clickhouse clickhouse-client --user homework --password boba_aboba --multiquery --format CSVWithNames < analytics/max_category_by_state.sql > analytics/max_category_by_state.csv
```

Остановка: `docker-compose stop`.
