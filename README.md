# Kafka и ClickHouse

Нужно загрузить тестовый csv в kafka и найти категорию самой большой транзакции в каждом штате. Таблицы создаются из `analytics/schema.sql`, запрос лежит в `analytics/max_category_by_state.sql`. используется `AggregatingMergeTree` как оптимизация.

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

Дождитесь, пока число строк в таблице совпадёт с числом, выведенным загрузчиком, затем сохраните результат в CSV:

```bash
docker-compose exec -T clickhouse clickhouse-client --user homework --password boba_aboba --query 'SELECT count() FROM transactions'
docker-compose exec -T clickhouse clickhouse-client --user homework --password boba_aboba --multiquery --format CSVWithNames < analytics/max_category_by_state.sql > analytics/max_category_by_state.csv
```

Повторная загрузка CSV добавит те же строки ещё раз. Остановка: `docker-compose stop`.
