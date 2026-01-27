-- Ozon Ads Dashboard View (Рекламный дашборд)
-- Объединяет данные из:
--   - ozon_campaign_product_stats (Оплата за клик)
--   - ozon_sku_promo_stats (Оплата за заказ)
--   - ozon_analytics_data (общая воронка для CR воронки)
-- Выполнить в Supabase SQL Editor

-- Сначала создаём таблицу ozon_sku_promo_stats если её ещё нет
CREATE TABLE IF NOT EXISTS ozon_sku_promo_stats (
    id                  BIGSERIAL,
    date                DATE NOT NULL,
    sku                 TEXT NOT NULL,
    product_name        TEXT,
    orders              INTEGER DEFAULT 0,
    quantity            INTEGER DEFAULT 0,
    revenue             NUMERIC(14,2) DEFAULT 0,
    cost                NUMERIC(14,2) DEFAULT 0,
    drr                 NUMERIC(10,2) DEFAULT 0,
    fetched_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, sku)
);

CREATE INDEX IF NOT EXISTS idx_ozon_sku_promo_stats_date ON ozon_sku_promo_stats(date);
CREATE INDEX IF NOT EXISTS idx_ozon_sku_promo_stats_sku ON ozon_sku_promo_stats(sku);


-- Создаём View с объединёнными данными рекламы
CREATE OR REPLACE VIEW ozon_ads_dashboard AS
SELECT
    p.date AS "Дата",
    p.sku AS "sku",
    SPLIT_PART(p.product_name, ',', 1) AS "название",
    p.impressions AS "Охваты",
    p.clicks AS "Клики",
    p.ctr AS "ctr",
    p.avg_cpc AS "cpc",
    -- Заказы: Оплата за клик (прямые + модель) + Оплата за заказ
    (p.orders + COALESCE(p.model_orders, 0) + COALESCE(s.orders, 0)) AS "Заказы",
    -- CR: все заказы / клики
    CASE
        WHEN p.clicks > 0
        THEN ROUND(((p.orders + COALESCE(p.model_orders, 0) + COALESCE(s.orders, 0))::numeric / p.clicks) * 100, 2)
        ELSE 0
    END AS "CR",
    -- Выручка: Оплата за клик (прямые + модель) + Оплата за заказ
    (p.revenue + COALESCE(p.model_revenue, 0) + COALESCE(s.revenue, 0)) AS "Выручка",
    -- Рекламный бюджет: сумма расходов
    (p.cost + COALESCE(s.cost, 0)) AS "Рекламный бюджет",
    -- ДРР: общий расход / общая выручка
    CASE
        WHEN (p.revenue + COALESCE(p.model_revenue, 0) + COALESCE(s.revenue, 0)) > 0
        THEN ROUND(((p.cost + COALESCE(s.cost, 0)) / (p.revenue + COALESCE(p.model_revenue, 0) + COALESCE(s.revenue, 0))) * 100, 2)
        ELSE 0
    END AS "дрр",
    -- CPO: расход / заказы
    CASE
        WHEN (p.orders + COALESCE(p.model_orders, 0) + COALESCE(s.orders, 0)) > 0
        THEN ROUND((p.cost + COALESCE(s.cost, 0)) / (p.orders + COALESCE(p.model_orders, 0) + COALESCE(s.orders, 0)), 2)
        ELSE 0
    END AS "cpo",
    -- CPM: расход / показы * 1000
    CASE
        WHEN p.impressions > 0
        THEN ROUND((p.cost / p.impressions) * 1000, 2)
        ELSE 0
    END AS "cpm",
    -- Доля рекламной выручки
    CASE
        WHEN COALESCE(a.revenue, 0) > 0
        THEN ROUND(((p.revenue + COALESCE(p.model_revenue, 0) + COALESCE(s.revenue, 0)) / a.revenue) * 100, 2)
        ELSE 0
    END AS "доля рекламной выручки",
    -- CR воронки: заказы из Analytics / клики в карточку
    CASE
        WHEN COALESCE(a.session_view_pdp, 0) > 0
        THEN ROUND((a.ordered_units::numeric / a.session_view_pdp) * 100, 2)
        ELSE 0
    END AS "CR воронки"
FROM ozon_campaign_product_stats p
LEFT JOIN ozon_sku_promo_stats s
    ON p.date = s.date AND p.sku = s.sku
LEFT JOIN ozon_analytics_data a
    ON p.date = a.date AND p.sku = a.sku
ORDER BY p.date DESC, p.sku;

GRANT SELECT ON ozon_ads_dashboard TO anon, authenticated;

COMMENT ON VIEW ozon_ads_dashboard IS 'Рекламный дашборд Ozon: объединяет Оплату за клик + Оплату за заказ. Заказы и выручка включают прямые и ассоциированные конверсии.';
