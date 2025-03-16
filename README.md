# ETL_PROJECT

## Что делает:

1) Генерация данных в MongoDB ->
2) Репликация данных в PostgreSQL ->
3) Создание аналитических витрин в ClickHouse.


### Выполняем установку:
```bash
git clone https://github.com/hamonuserr/ETL_PROJECT.git
docker-compose up --build
```

### Используемые порты:

- MongoDB: 27017
- PostgreSQL: 5432
- Airflow UI: 8080
- ClickHouse: 8123

### DAGs:
1) Генерация данных каждые 10 минут  
2) Репликация в PostgreSQL каждые 60 мин 
3) Аналитические витрины в 00:00 (таблицы : `user_activity_daily`, `device_usage_stats`, `support_performance`)