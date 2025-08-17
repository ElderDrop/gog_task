-- Yearly Revenue Trends Analysis per Genre
-- This analysis detects revenue trends by comparing yearly performance across genres

WITH yearly_revenue AS (
    SELECT 
        DATE_TRUNC('year', transaction_date) as year,
        genre,
        SUM(daily_revenue) as yearly_revenue
    FROM {{ ref('fact_daily_revenue') }}
    GROUP BY DATE_TRUNC('year', transaction_date), genre
),

yearly_revenue_with_lag AS (
    SELECT 
        year,
        genre,
        yearly_revenue,
        LAG(yearly_revenue, 1) OVER (PARTITION BY genre ORDER BY year) as prev_year_revenue,
        LAG(yearly_revenue, 2) OVER (PARTITION BY genre ORDER BY year) as two_years_ago_revenue
    FROM yearly_revenue
),

trend_analysis AS (
    SELECT 
        year,
        genre,
        yearly_revenue,
        prev_year_revenue,
        two_years_ago_revenue,
        
        -- Year-over-year change
        CASE 
            WHEN prev_year_revenue IS NOT NULL THEN 
                ROUND(((yearly_revenue - prev_year_revenue) / prev_year_revenue)::numeric * 100, 2)
            ELSE NULL 
        END as yoy_change_percent,
        
        -- Two-year trend (to identify sustained trends)
        CASE 
            WHEN two_years_ago_revenue IS NOT NULL THEN 
                ROUND(((yearly_revenue - two_years_ago_revenue) / two_years_ago_revenue)::numeric * 100, 2)
            ELSE NULL 
        END as two_year_change_percent,
        
        -- Trend classification
        CASE 
            WHEN prev_year_revenue IS NULL THEN 'New'
            WHEN yearly_revenue > prev_year_revenue THEN 'Growing'
            WHEN yearly_revenue < prev_year_revenue THEN 'Declining'
            ELSE 'Stable'
        END as trend_direction,
        
        -- Trend strength classification
        CASE 
            WHEN prev_year_revenue IS NULL THEN 'New'
            WHEN ABS((yearly_revenue - prev_year_revenue) / prev_year_revenue) > 0.20 THEN 'Strong'
            WHEN ABS((yearly_revenue - prev_year_revenue) / prev_year_revenue) > 0.10 THEN 'Moderate'
            ELSE 'Weak'
        END as trend_strength
        
    FROM yearly_revenue_with_lag
)

SELECT 
    year,
    genre,
    ROUND(yearly_revenue::numeric, 2) as yearly_revenue,
    yoy_change_percent,
    two_year_change_percent,
    trend_direction,
    trend_strength,
    
    -- Additional insights
    CASE 
        WHEN trend_direction = 'Growing' AND trend_strength = 'Strong' THEN '🚀 Strong Growth'
        WHEN trend_direction = 'Growing' AND trend_strength = 'Moderate' THEN '📈 Moderate Growth'
        WHEN trend_direction = 'Declining' AND trend_strength = 'Strong' THEN '📉 Strong Decline'
        WHEN trend_direction = 'Declining' AND trend_strength = 'Moderate' THEN '📊 Moderate Decline'
        WHEN trend_direction = 'Stable' THEN '➡️ Stable'
        ELSE '🆕 New'
    END as trend_summary
    
FROM trend_analysis
ORDER BY genre, year DESC
