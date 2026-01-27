-- Ozon Dashboard View
-- Объединяет данные из ozon_analytics_data и ozon_campaign_product_stats
-- Выполнить в Supabase SQL Editor

CREATE OR REPLACE VIEW ozon_dashboard AS
SELECT
    a.date AS "Дата",
    a.sku AS "sku",
    a.product_name AS "название",
    a.session_view_pdp AS "Клики",
    a.ordered_units AS "Заказы",
    CASE
        WHEN a.session_view_pdp > 0
        THEN ROUND((a.ordered_units::numeric / a.session_view_pdp) * 100, 2)
        ELSE 0
    END AS "CR",
    a.revenue AS "Выручка",
    COALESCE(p.cost, 0) AS "Рекламный бюджет",
    p.price AS "Цена",
    NULL::numeric AS "Соинвест",
    CASE
        WHEN a.revenue > 0
        THEN ROUND((COALESCE(p.cost, 0)::numeric / a.revenue) * 100, 2)
        ELSE 0
    END AS "дрр"
FROM ozon_analytics_data a
LEFT JOIN (
    SELECT date, sku,
           SUM(cost) as cost,
           AVG(price) as price
    FROM ozon_campaign_product_stats
    GROUP BY date, sku
) p ON a.date = p.date AND a.sku = p.sku
ORDER BY a.date DESC, a.sku;

-- Даём доступ через API
GRANT SELECT ON ozon_dashboard TO anon, authenticated;

COMMENT ON VIEW ozon_dashboard IS 'Дашборд Ozon: клики, заказы, CR, выручка, рекламный бюджет, ДРР';
