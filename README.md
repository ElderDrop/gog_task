### ⚙️ GOG Task Implementation

This document outlines the implementation of the GOG data pipeline, including the data model, cleaning procedures, and key analytical findings.

***

### 1. Data Model Design

The data pipeline utilizes a two-layered architecture: a **Staging Layer** for initial data processing and a **Data Mart Layer** for refined, analysis-ready data.

* **Staging Layer**: This layer is designed for **data cleansing**. Tables were created to process raw data by applying **trimming** and **normalization**, and performing **explicit type casting** to ensure data integrity and a consistent structure before it moves to the data mart.

* **Data Mart Layer**: This layer contains the final, structured tables for business intelligence and reporting.
    * **`dim_games`**: A dimension table for game-related information. It uses a **surrogate key** generated during the staging process. The original `game_id` is preserved under an alias. In a typical production setup, developer information would be a separate dimension.
    * **`fact_daily_revenue`**: A fact table storing daily revenue. This table was populated by filtering out **invalid game entries** and excluding transactions with **missing exchange rates** from the source data. The assumption is that transactions without an exchange rate should be excluded.

***

### 2. Analytical Insights

The `analyses` folder contains SQL files and CSV results that provide valuable business insights.

* **Missing Exchange Rates**:
    * **File**: `missing_exchange_rate_for_trans.sql`
    * **Finding**: Several transactions lack exchange rate data, which could point to a **data quality issue**, especially if the source system only provides exchange rate changes.

* **Lost Subscription Revenue**:
    * **File**: `lost_subscription_revenue.sql`
    * **Assumption**: A subscription is approximately one month long.
    * **Finding**: By estimating the potential revenue from users who haven't renewed their subscription in over 60 days, we found that about **34,000 PLN** could be recovered. On average, this amounts to roughly **214 PLN per game** with a subscription.

* **Interest Trends**:
    * **File**: `intrest.sql`
    * **Finding**: The analysis shows a **declining trend in revenue across all genres** on a year-to-year basis, which may indicate a broader market issue. A more granular, month-to-month analysis could help in **planning targeted marketing strategies** to boost sales during specific periods.

***

### 3. Data Reconciliation

The reconciliation logic for `raw_transactions` vs. `psp_transactions` was integrated directly into the fact table layer. This strategic placement was made because the data is crucial for future analytical purposes, such as identifying game categories with **payment processing issues** or pinpointing problems with the integration system.