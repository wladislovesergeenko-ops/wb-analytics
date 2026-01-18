-- ============================================================
-- WB Analytics: SQL для создания новых таблиц
-- Дата: 2026-01-19
-- ============================================================

-- ============================================================
-- 1. wb_tariffs_commission
-- Комиссии WB по категориям (обновляется еженедельно)
-- ============================================================

CREATE TABLE IF NOT EXISTS wb_tariffs_commission (
    subject_id      BIGINT PRIMARY KEY,              -- ID категории (уникальный ключ)
    subject_name    TEXT,                            -- Название категории
    parent_id       BIGINT,                          -- ID родительской категории
    parent_name     TEXT,                            -- Название родительской категории
    commission_fbs  NUMERIC(5,2),                    -- Комиссия FBS (Marketplace), %
    commission_fbw  NUMERIC(5,2),                    -- Комиссия FBW (Paid Storage), %
    commission_dbs  NUMERIC(5,2),                    -- Комиссия DBS (Supplier), %
    commission_edbs NUMERIC(5,2),                    -- Комиссия EDBS (Express), %
    commission_booking NUMERIC(5,2),                 -- Комиссия Бронирование, %
    commission_pickup NUMERIC(5,2),                  -- Комиссия C&C (Pickup), %
    updated_at      TIMESTAMPTZ DEFAULT NOW()        -- Время последнего обновления
);

-- Индекс для поиска по родительской категории
CREATE INDEX IF NOT EXISTS idx_wb_tariffs_commission_parent
    ON wb_tariffs_commission(parent_id);

-- Комментарии
COMMENT ON TABLE wb_tariffs_commission IS 'Комиссии WB по категориям товаров. Обновляется еженедельно (UPSERT).';
COMMENT ON COLUMN wb_tariffs_commission.commission_fbs IS 'Комиссия по модели FBS (Marketplace), %';
COMMENT ON COLUMN wb_tariffs_commission.commission_fbw IS 'Комиссия по модели FBW (склад WB), %';
COMMENT ON COLUMN wb_tariffs_commission.commission_dbs IS 'Комиссия по модели DBS (со склада продавца), %';


-- ============================================================
-- 2. wb_search_report_products
-- Отчёт по поисковым позициям товаров (ежедневно)
-- ============================================================

CREATE TABLE IF NOT EXISTS wb_search_report_products (
    id                      BIGSERIAL,                   -- Внутренний ID
    period_start            DATE NOT NULL,               -- Начало периода
    period_end              DATE NOT NULL,               -- Конец периода
    nm_id                   BIGINT NOT NULL,             -- ID товара (nmId)

    -- Группировка
    subject_id              BIGINT,                      -- ID предмета
    subject_name            TEXT,                        -- Название предмета
    brand_name              TEXT,                        -- Бренд
    tag_id                  BIGINT,                      -- ID тега
    tag_name                TEXT,                        -- Название тега

    -- Информация о товаре
    name                    TEXT,                        -- Название товара
    vendor_code             TEXT,                        -- Артикул продавца
    is_advertised           BOOLEAN,                     -- Рекламируется ли
    is_card_rated           BOOLEAN,                     -- Есть ли оценка карточки
    rating                  NUMERIC(3,1),                -- Оценка карточки
    feedback_rating         NUMERIC(3,1),                -- Рейтинг отзывов
    price_min               INTEGER,                     -- Минимальная цена
    price_max               INTEGER,                     -- Максимальная цена

    -- Метрики позиции
    avg_position            INTEGER,                     -- Средняя позиция в поиске
    avg_position_dynamics   INTEGER,                     -- Динамика позиции (%)
    visibility              INTEGER,                     -- Видимость в поиске (%)
    visibility_dynamics     INTEGER,                     -- Динамика видимости (%)

    -- Метрики конверсии
    open_card               INTEGER,                     -- Переходы в карточку
    open_card_dynamics      INTEGER,                     -- Динамика переходов (%)
    add_to_cart             INTEGER,                     -- Добавления в корзину
    add_to_cart_dynamics    INTEGER,                     -- Динамика корзины (%)
    open_to_cart            INTEGER,                     -- Конверсия открытие->корзина
    open_to_cart_dynamics   INTEGER,                     -- Динамика конверсии (%)
    orders                  INTEGER,                     -- Заказы из поиска
    orders_dynamics         INTEGER,                     -- Динамика заказов (%)
    cart_to_order           INTEGER,                     -- Конверсия корзина->заказ
    cart_to_order_dynamics  INTEGER,                     -- Динамика (%)

    created_at              TIMESTAMPTZ DEFAULT NOW(),   -- Время создания записи

    -- Уникальный ключ: товар + период
    PRIMARY KEY (nm_id, period_start, period_end)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_wb_search_report_products_period
    ON wb_search_report_products(period_start, period_end);

CREATE INDEX IF NOT EXISTS idx_wb_search_report_products_subject
    ON wb_search_report_products(subject_id);

CREATE INDEX IF NOT EXISTS idx_wb_search_report_products_brand
    ON wb_search_report_products(brand_name);

-- Комментарии
COMMENT ON TABLE wb_search_report_products IS 'Отчёт по поисковым позициям товаров. Загружается ежедневно.';
COMMENT ON COLUMN wb_search_report_products.avg_position IS 'Средняя позиция товара в поисковой выдаче';
COMMENT ON COLUMN wb_search_report_products.visibility IS 'Процент видимости товара в поиске';


-- ============================================================
-- 3. wb_product_search_texts
-- Поисковые запросы по товарам (ежедневно)
-- ============================================================

CREATE TABLE IF NOT EXISTS wb_product_search_texts (
    id                      BIGSERIAL,                   -- Внутренний ID
    period_start            DATE NOT NULL,               -- Начало периода
    period_end              DATE NOT NULL,               -- Конец периода
    nm_id                   BIGINT NOT NULL,             -- ID товара (nmId)
    text                    TEXT NOT NULL,               -- Поисковый запрос

    -- Информация о товаре
    subject_name            TEXT,                        -- Название предмета
    brand_name              TEXT,                        -- Бренд
    vendor_code             TEXT,                        -- Артикул продавца
    name                    TEXT,                        -- Название товара
    is_card_rated           BOOLEAN,                     -- Есть ли оценка карточки
    rating                  NUMERIC(3,1),                -- Оценка карточки
    feedback_rating         NUMERIC(3,1),                -- Рейтинг отзывов
    price_min               INTEGER,                     -- Минимальная цена
    price_max               INTEGER,                     -- Максимальная цена

    -- Частотность запроса
    frequency               INTEGER,                     -- Частотность за период
    frequency_dynamics      INTEGER,                     -- Динамика частотности (%)
    week_frequency          INTEGER,                     -- Недельная частотность

    -- Метрики позиции
    median_position         INTEGER,                     -- Медианная позиция
    median_position_dynamics INTEGER,                    -- Динамика медианной позиции (%)
    avg_position            INTEGER,                     -- Средняя позиция
    avg_position_dynamics   INTEGER,                     -- Динамика средней позиции (%)

    -- Метрики конверсии с перцентилями
    open_card               INTEGER,                     -- Переходы в карточку
    open_card_dynamics      INTEGER,                     -- Динамика (%)
    open_card_percentile    INTEGER,                     -- Перцентиль среди конкурентов
    add_to_cart             INTEGER,                     -- Добавления в корзину
    add_to_cart_dynamics    INTEGER,                     -- Динамика (%)
    add_to_cart_percentile  INTEGER,                     -- Перцентиль
    open_to_cart            INTEGER,                     -- Конверсия открытие->корзина
    open_to_cart_dynamics   INTEGER,                     -- Динамика (%)
    open_to_cart_percentile INTEGER,                     -- Перцентиль
    orders                  INTEGER,                     -- Заказы
    orders_dynamics         INTEGER,                     -- Динамика (%)
    orders_percentile       INTEGER,                     -- Перцентиль
    cart_to_order           INTEGER,                     -- Конверсия корзина->заказ
    cart_to_order_dynamics  INTEGER,                     -- Динамика (%)
    cart_to_order_percentile INTEGER,                    -- Перцентиль
    visibility              INTEGER,                     -- Видимость (%)
    visibility_dynamics     INTEGER,                     -- Динамика видимости (%)

    created_at              TIMESTAMPTZ DEFAULT NOW(),   -- Время создания записи

    -- Уникальный ключ: товар + запрос + период
    PRIMARY KEY (nm_id, text, period_start, period_end)
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_wb_product_search_texts_period
    ON wb_product_search_texts(period_start, period_end);

CREATE INDEX IF NOT EXISTS idx_wb_product_search_texts_nm_id
    ON wb_product_search_texts(nm_id);

CREATE INDEX IF NOT EXISTS idx_wb_product_search_texts_text
    ON wb_product_search_texts USING gin(to_tsvector('russian', text));

CREATE INDEX IF NOT EXISTS idx_wb_product_search_texts_frequency
    ON wb_product_search_texts(frequency DESC);

-- Комментарии
COMMENT ON TABLE wb_product_search_texts IS 'Поисковые запросы по товарам с метриками конверсии. Загружается ежедневно.';
COMMENT ON COLUMN wb_product_search_texts.text IS 'Поисковый запрос пользователя';
COMMENT ON COLUMN wb_product_search_texts.frequency IS 'Частотность запроса за период';
COMMENT ON COLUMN wb_product_search_texts.open_card_percentile IS 'Перцентиль по переходам среди конкурентов (0-100)';


-- ============================================================
-- Полезные запросы для проверки данных
-- ============================================================

-- Проверка комиссий для БАДов и пищевых добавок:
-- SELECT * FROM wb_tariffs_commission
-- WHERE subject_name ILIKE '%бад%' OR subject_name ILIKE '%добавк%' OR parent_name ILIKE '%бад%';

-- Топ-10 поисковых запросов по частотности:
-- SELECT text, frequency, nm_id FROM wb_product_search_texts
-- ORDER BY frequency DESC LIMIT 10;

-- Товары с лучшей позицией в поиске:
-- SELECT nm_id, name, avg_position, visibility FROM wb_search_report_products
-- WHERE period_start = '2026-01-18' ORDER BY avg_position ASC LIMIT 20;
