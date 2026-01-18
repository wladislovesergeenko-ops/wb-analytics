# WB-Analytics: Статус проекта

> **Последнее обновление:** 2026-01-19
> **Версия:** v2.1

## Краткое описание

ETL-приложение для извлечения аналитических данных с маркетплейсов **Wildberries** и **Ozon** в базу данных **Supabase**.

---

## Архитектура проекта

```
src/
├── config/settings.py        # Pydantic-конфигурация (.env)
├── core/
│   ├── base_connector.py     # Абстрактный коннектор
│   └── exceptions.py         # Кастомные исключения
├── connectors/
│   ├── wb.py                 # Wildberries API
│   ├── ozon.py               # Ozon Analytics API
│   └── ozon_performance.py   # Ozon Performance API (реклама)
├── etl/
│   ├── main.py               # Главная точка входа
│   ├── transformers.py       # WB трансформеры
│   ├── ozon_transformer.py   # Ozon трансформеры
│   └── ozon_performance_transformer.py  # CSV парсер Ozon
├── logging_config/logger.py  # Логирование
└── utils/retry.py            # Retry-декоратор

scripts/
├── test_new_methods.py       # Тестирование новых API методов
└── load_historical_data.py   # Загрузка исторических данных

sql/
└── create_tables_new.sql     # SQL для новых таблиц
```

---

## API интеграции

### Wildberries — Основные методы (v1)

| Эндпоинт | Назначение | Таблица Supabase | Частота |
|----------|------------|------------------|---------|
| Sales Funnel | Воронка продаж | `wb_sales_funnel_products` | Ежедневно |
| Adverts | Рекламные кампании | `wb_adverts_nm_settings` | Ежедневно |
| Fullstats | Статистика РК по дням | `wb_adv_fullstats_daily` | Ежедневно |
| Orders | SPP снапшот | `wb_spp_daily` | Ежедневно |

### Wildberries — Новые методы (v2.1)

| Эндпоинт | Назначение | Таблица Supabase | Частота |
|----------|------------|------------------|---------|
| Tariffs Commission | Комиссии по категориям | `wb_tariffs_commission` | Еженедельно |
| Search Report | Позиции товаров в поиске | `wb_search_report_products` | Ежедневно |
| Product Search Texts | Поисковые запросы по товарам | `wb_product_search_texts` | Ежедневно |

**Аутентификация:** Bearer token (JWT)
**Rate limits:**
- Комиссии: 1 req/min
- Search Report / Search Texts: 3 req/min (20 сек интервал)

### Ozon Analytics API (Коннектор готов)

| Эндпоинт | Назначение | Таблица Supabase |
|----------|------------|------------------|
| `/v1/analytics/data` | Аналитика: выручка, заказы, просмотры | *Не создана* |

### Ozon Performance API (Коннектор готов)

| Эндпоинт | Назначение | Таблица Supabase |
|----------|------------|------------------|
| Campaigns | Список рекламных кампаний | *Не создана* |
| Statistics | Асинхронные отчёты по статистике | *Не создана* |

---

## Текущее состояние

### Wildberries — Основные пайплайны

| Компонент | Статус | Примечания |
|-----------|--------|------------|
| Sales Funnel pipeline | ✅ Готово | Ежедневная загрузка |
| Adverts pipeline | ✅ Готово | Полное обновление |
| Fullstats pipeline | ✅ Готово | Ежедневная загрузка |
| SPP pipeline | ✅ Готово | Ежедневный снапшот |
| GitHub Actions | ✅ Готово | 03:10 UTC ежедневно |

### Wildberries — Новые методы (v2.1)

| Компонент | Статус | Примечания |
|-----------|--------|------------|
| `fetch_tariffs_commission` | ✅ Готово | Коннектор + трансформер |
| `fetch_search_report` | ✅ Готово | С пагинацией |
| `fetch_product_search_texts` | ✅ Готово | С чанкингом (max 50 nmIds) |
| Таблицы в Supabase | ✅ Созданы | 3 новых таблицы |
| Исторические данные | ✅ Загружены | 01.01 — 18.01.2026 |
| Ежедневный пайплайн | ✅ Готово | Добавлено в main.py |
| GitHub Actions | ✅ Готово | etl_daily.yml обновлён |

#### Загруженные исторические данные (01.01 — 18.01.2026)

| Таблица | Записей |
|---------|---------|
| `wb_tariffs_commission` | 7 346 |
| `wb_search_report_products` | 742 |
| `wb_product_search_texts` | 4 833 |

### Ozon (Частично реализовано)

| Компонент | Статус | Примечания |
|-----------|--------|------------|
| OzonConnector | ✅ Готово | Аналитика API |
| OzonPerformanceConnector | ✅ Готово | Performance API + OAuth |
| Ozon pipelines | ❌ Не готово | Нет интеграции в main.py |
| Ozon таблицы | ❌ Не созданы | Требуется проектирование |

---

## Таблицы Supabase

### Wildberries — Основные

#### `wb_sales_funnel_products`
```sql
-- Ключ: nmid, periodstart, periodend
nmid, title, vendorcode, brandname, subjectid, subjectname,
feedbackrating, stocks, opencount, cartcount, ordercount,
ordersum, buyoutcount, buyoutsum, cancelcount, cancelsum,
avgprice, localizationpercent, periodstart, periodend
```

#### `wb_adverts_nm_settings`
```sql
-- Ключ: advert_id, nmid
advert_id, nmid, status, bid_type, payment_type, campaign_name,
place_search, place_recommendations, bid_search_kopecks,
bid_recommendations_kopecks, subject_id, subject_name,
ts_created, ts_started, ts_updated, ts_deleted
```

#### `wb_adv_fullstats_daily`
```sql
-- Ключ: advert_id, date
advert_id, date, atbs, views, clicks, orders, canceled, shks,
sum, sum_price, cpc, ctr, cr, raw
```

#### `wb_spp_daily`
```sql
-- Ключ: date, nmid
date, nmid, spp, finished_price
```

### Wildberries — Новые (v2.1)

#### `wb_tariffs_commission`
```sql
-- Ключ: subject_id (UPSERT еженедельно)
subject_id, subject_name, parent_id, parent_name,
commission_fbs, commission_fbw, commission_dbs,
commission_edbs, commission_booking, commission_pickup,
updated_at
```

#### `wb_search_report_products`
```sql
-- Ключ: nm_id, period_start, period_end
nm_id, period_start, period_end, subject_id, subject_name,
brand_name, tag_id, tag_name, name, vendor_code,
is_advertised, is_card_rated, rating, feedback_rating,
price_min, price_max, avg_position, avg_position_dynamics,
open_card, add_to_cart, open_to_cart, orders, cart_to_order,
visibility, *_dynamics
```

#### `wb_product_search_texts`
```sql
-- Ключ: nm_id, text, period_start, period_end
nm_id, text, period_start, period_end, subject_name, brand_name,
vendor_code, name, frequency, frequency_dynamics, week_frequency,
median_position, avg_position, open_card, add_to_cart, orders,
visibility, *_dynamics, *_percentile
```

### Ozon (Планируемые)

| Таблица | Назначение | Статус |
|---------|------------|--------|
| `ozon_analytics_daily` | Аналитика по дням/SKU | ❌ Не создана |
| `ozon_campaigns` | Рекламные кампании | ❌ Не создана |
| `ozon_campaign_stats` | Статистика РК | ❌ Не создана |

---

## Скрипты

### Тестирование новых методов
```bash
python3 scripts/test_new_methods.py
```

### Загрузка исторических данных
```bash
# Всё сразу
python3 scripts/load_historical_data.py --all

# Только комиссии
python3 scripts/load_historical_data.py --commission

# Только search_report за период
python3 scripts/load_historical_data.py --search-report --start 2026-01-01 --end 2026-01-18

# Только search_texts (требует загруженный search_report)
python3 scripts/load_historical_data.py --search-texts --start 2026-01-01 --end 2026-01-18
```

---

## Конфигурация (.env)

```env
# Supabase
SUPABASE_URL=https://...supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJ...

# Wildberries
WB_KEY=eyJ...

# Ozon
OZON_API_KEY=...
OZON_CLIENT_ID=...
OZON_PERF_CLIENT_ID=...
OZON_PERF_CLIENT_SECRET=...
OZON_ENABLED=true

# ETL флаги (основные)
RUN_SALES_FUNNEL=1
RUN_ADVERTS_SETTINGS=1
RUN_ADVERTS_FULLSTATS=1
RUN_SPP=1
RUN_OZON_PRODUCTS=false

# ETL флаги (новые) — TODO: добавить в main.py
RUN_SEARCH_REPORT=1
RUN_SEARCH_TEXTS=1
RUN_COMMISSION=0  # Еженедельно

# Настройки
SLEEP_SECONDS=21
OVERLAP_DAYS=2
```

---

## Следующие шаги

### Приоритет 1: Завершить интеграцию новых WB методов
- [x] Добавить методы в коннектор
- [x] Создать трансформеры
- [x] Создать таблицы в Supabase
- [x] Загрузить комиссии (7 346 записей)
- [x] Загрузить search_report за 1-18 января (742 записи)
- [x] Загрузить search_texts за 1-18 января (4 833 записи)
- [x] Добавить пайплайны в main.py
- [x] Настроить GitHub Actions

### Приоритет 2: Ozon Analytics
- [ ] Спроектировать схему `ozon_analytics_daily`
- [ ] Добавить pipeline в main.py
- [ ] Тестирование загрузки

### Приоритет 3: Ozon Performance
- [ ] Спроектировать схему для кампаний и статистики
- [ ] Реализовать асинхронную загрузку отчётов
- [ ] Добавить pipeline в main.py

---

## Запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Локальный запуск основного ETL
python -m src.etl.main

# Тесты
pytest tests/
```

---

## Контакты и ресурсы

- **GitHub Actions:** `.github/workflows/etl_daily.yml`
- **Логи:** `logs/` (ротация 10MB, 5 бэкапов)
- **Supabase Dashboard:** см. SUPABASE_URL в .env
- **SQL миграции:** `sql/create_tables_new.sql`
