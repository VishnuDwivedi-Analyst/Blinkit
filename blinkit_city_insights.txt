
create database Blinkit
SELECT DB_NAME() AS CurrentDatabase;
USE [Blinkit];
GO 
select count(*) from all_blinkit_category_scraping_stream
SELECT TOP 10 * FROM [dbo].[all_blinkit_category_scraping_stream];
SELECT TOP 3 * FROM [dbo].[blinkit_categories];
SELECT TOP 3 * FROM [dbo].[blinkit_city_map];



SELECT 
    store_id,
    sku_id,
    sku_name,
    created_at,
    inventory,
    LAG(inventory) OVER (PARTITION BY store_id, sku_id ORDER BY created_at) AS prev_inventory
INTO #lagged_inventory
FROM dbo.all_blinkit_category_scraping_stream;


SELECT *,
    CASE 
        WHEN prev_inventory IS NULL THEN NULL
        WHEN inventory < prev_inventory THEN prev_inventory - inventory
        WHEN inventory > prev_inventory THEN NULL
        ELSE 0
    END AS raw_est_qty_sold
INTO #base_sales
FROM #lagged_inventory;


SELECT 
    bs.store_id,
    bs.sku_id,
    bs.sku_name,
    bs.created_at,
    bs.inventory,
    ISNULL(bs.raw_est_qty_sold, prev.avg_qty) AS est_qty_sold
FROM #base_sales bs
OUTER APPLY (
    SELECT AVG(raw_est_qty_sold * 1.0) AS avg_qty
    FROM (
        SELECT TOP 3 raw_est_qty_sold
        FROM #base_sales
        WHERE sku_id = bs.sku_id
          AND store_id = bs.store_id
          AND created_at < bs.created_at
          AND raw_est_qty_sold IS NOT NULL
        ORDER BY created_at DESC
    ) AS last3
) AS prev
ORDER BY bs.store_id, bs.sku_id, bs.created_at;


DROP TABLE IF EXISTS #lagged_inventory;
DROP TABLE IF EXISTS #base_sales;


SELECT 
    store_id,
    sku_id,
    sku_name,
    created_at,
    inventory,
    LAG(inventory) OVER (PARTITION BY store_id, sku_id ORDER BY created_at) AS prev_inventory
INTO #lagged_inventory
FROM dbo.all_blinkit_category_scraping_stream;

-- Add estimated sales logic
SELECT *,
    CASE 
        WHEN prev_inventory IS NULL THEN NULL
        WHEN inventory < prev_inventory THEN prev_inventory - inventory
        WHEN inventory > prev_inventory THEN NULL
        ELSE 0
    END AS raw_est_qty_sold
INTO #base_sales
FROM #lagged_inventory;


WITH enriched_data AS (
    SELECT 
        bs.store_id,
        city.city_name,
        bs.sku_id,
        bs.sku_name,
        bs.created_at AS date,
        bs.inventory,
        bs.raw_est_qty_sold,
        ISNULL(bs.raw_est_qty_sold, est.est_qty_sold) AS est_qty_sold,
        base.brand_id,
        base.brand,
        base.mrp,
        base.selling_price,
        base.image_url,
        base.l1_category_id,
        base.l2_category_id,
        cat.l1_category,
        cat.l2_category
    FROM #base_sales bs
    OUTER APPLY (
        SELECT AVG(raw_est_qty_sold * 1.0) AS est_qty_sold
        FROM (
            SELECT TOP 3 raw_est_qty_sold
            FROM #base_sales prev
            WHERE prev.sku_id = bs.sku_id
              AND prev.store_id = bs.store_id
              AND prev.created_at < bs.created_at
              AND raw_est_qty_sold IS NOT NULL
            ORDER BY created_at DESC
        ) AS last3
    ) AS est
    INNER JOIN (
        SELECT DISTINCT store_id, sku_id, sku_name, created_at, brand_id, brand, mrp, selling_price, image_url, l1_category_id, l2_category_id
        FROM dbo.all_blinkit_category_scraping_stream
    ) base ON bs.store_id = base.store_id AND bs.sku_id = base.sku_id AND bs.created_at = base.created_at
    LEFT JOIN dbo.blinkit_city_map city ON bs.store_id = city.store_id
    LEFT JOIN dbo.blinkit_categories cat 
        ON base.l1_category_id = cat.l1_category_id 
       AND base.l2_category_id = cat.l2_category_id
    WHERE city.city_name IS NOT NULL
)

SELECT TOP 1000 * FROM enriched_data;



WITH enriched_data AS (
    SELECT 
        bs.store_id,
        city.city_name,
        bs.sku_id,
        bs.sku_name,
        bs.created_at AS date,
        bs.inventory,
        bs.raw_est_qty_sold,
        ISNULL(bs.raw_est_qty_sold, est.est_qty_sold) AS est_qty_sold,
        base.brand_id,
        base.brand,
        base.mrp,
        base.selling_price,
        base.image_url,
        base.l1_category_id,
        base.l2_category_id,
        cat.l1_category,
        cat.l2_category
    FROM #base_sales bs
    OUTER APPLY (
        SELECT AVG(raw_est_qty_sold * 1.0) AS est_qty_sold
        FROM (
            SELECT TOP 3 raw_est_qty_sold
            FROM #base_sales prev
            WHERE prev.sku_id = bs.sku_id
              AND prev.store_id = bs.store_id
              AND prev.created_at < bs.created_at
              AND raw_est_qty_sold IS NOT NULL
            ORDER BY created_at DESC
        ) AS last3
    ) AS est
    INNER JOIN (
        SELECT DISTINCT store_id, sku_id, sku_name, created_at, brand_id, brand, mrp, selling_price, image_url, l1_category_id, l2_category_id
        FROM dbo.all_blinkit_category_scraping_stream
    ) base ON bs.store_id = base.store_id AND bs.sku_id = base.sku_id AND bs.created_at = base.created_at
    LEFT JOIN dbo.blinkit_city_map city ON bs.store_id = city.store_id
    LEFT JOIN dbo.blinkit_categories cat 
        ON base.l1_category_id = cat.l1_category_id 
       AND base.l2_category_id = cat.l2_category_id
    WHERE city.city_name IS NOT NULL
)

SELECT
    store_id,
    city_name,
    sku_id,
    sku_name,
    date,
    l1_category_id,
    l1_category,
    l2_category_id,
    l2_category,
    brand_id,
    brand,
    mrp,
    selling_price,
    image_url,
    est_qty_sold,
    est_qty_sold * selling_price AS est_sales_sp,
    est_qty_sold * mrp AS est_sales_mrp,
    CASE 
        WHEN mrp > 0 THEN ROUND((mrp - selling_price) / mrp, 2)
        ELSE NULL
    END AS discount_pct,
    CASE 
        WHEN inventory > 0 THEN 1 ELSE 0
    END AS osa_flag
INTO dbo.blinkit_city_insights
FROM enriched_data;
SELECT * FROM dbo.blinkit_city_insights;
