# Blinkit Sales Insights SQL Project

## Overview
This project builds an enriched dataset of Blinkit product sales using SQL transformations.  
It estimates sales quantities from inventory changes, joins category/brand/city mappings,  
and outputs final city-level insights for analysis.

## Key Features
- Inventory tracking with LAG()
- Estimated quantity sold (est_qty_sold)
- Sales revenue calculations (MRP & SP)
- Discount percentage calculation
- On-Shelf Availability (OSA) flagging

## Final Output Table
`blinkit_city_insights` with fields:
- store_id, city_name, sku_id, sku_name
- category hierarchy (L1 & L2)
- brand, MRP, Selling Price
- est_qty_sold, est_sales_sp, est_sales_mrp
- discount_pct, osa_flag

## Usage
1. Run `Blinkit_Project.sql` in SQL Server.
2. Ensure the base tables:
   - `all_blinkit_category_scraping_stream`
   - `blinkit_categories`
   - `blinkit_city_map`
   are available in the `Blinkit` database.

## License
Distributed under the MIT License.
