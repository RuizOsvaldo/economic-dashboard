-- ============================================
-- ECONOMIC INDICATORS ANALYSIS QUERIES
-- Author: Osvaldo Ruiz
-- ============================================

-- ============================================
-- 1. CURRENT ECONOMIC SNAPSHOT
-- Latest values with trend indicators
-- ============================================
SELECT 
    title as "Indicator",
    ROUND(current_value::numeric, 2) as "Current Value",
    units as "Units",
    ROUND(mom_change::numeric, 2) as "MoM Change %",
    ROUND(yoy_change::numeric, 2) as "YoY Change %",
    status as "Status",
    as_of_date as "As Of"
FROM current_snapshot_view
ORDER BY category, title;


-- ============================================
-- 2. RECESSION PROBABILITY INDICATOR
-- Based on yield curve inversion
-- ============================================
WITH yield_curve AS (
    SELECT 
        observation_date,
        value as spread,
        rolling_avg_3m,
        LAG(value, 1) OVER (ORDER BY observation_date) as prev_month,
        LAG(value, 12) OVER (ORDER BY observation_date) as prev_year
    FROM calculated_metrics
    WHERE series_id = 'T10Y2Y'
    ORDER BY observation_date DESC
    LIMIT 36
)
SELECT 
    observation_date as "Date",
    ROUND(spread::numeric, 2) as "Yield Spread",
    CASE 
        WHEN spread < 0 THEN '🔴 INVERTED - Recession Signal'
        WHEN spread < 0.25 THEN '🟡 WARNING - Flat Curve'
        WHEN spread < 0.75 THEN '🟢 NORMAL'
        ELSE '🟢 STEEP - Growth Signal'
    END as "Signal",
    ROUND(rolling_avg_3m::numeric, 2) as "3M Average"
FROM yield_curve
ORDER BY observation_date DESC
LIMIT 12;


-- ============================================
-- 3. INFLATION ANALYSIS
-- CPI trends and Fed response
-- ============================================
WITH inflation_fed AS (
    SELECT 
        cm.observation_date,
        MAX(CASE WHEN cm.series_id = 'CPIAUCSL' THEN cm.yoy_change END) as inflation,
        MAX(CASE WHEN cm.series_id = 'FEDFUNDS' THEN cm.value END) as fed_rate,
        MAX(CASE WHEN cm.series_id = 'PCEPI' THEN cm.yoy_change END) as pce_inflation
    FROM calculated_metrics cm
    WHERE cm.observation_date >= '2020-01-01'
    GROUP BY cm.observation_date
)
SELECT 
    observation_date as "Date",
    ROUND(inflation::numeric, 2) as "CPI YoY %",
    ROUND(pce_inflation::numeric, 2) as "PCE YoY %",
    ROUND(fed_rate::numeric, 2) as "Fed Funds %",
    ROUND((fed_rate - inflation)::numeric, 2) as "Real Rate",
    CASE 
        WHEN inflation > 4 THEN '🔴 High Inflation'
        WHEN inflation > 2.5 THEN '🟡 Above Target'
        WHEN inflation >= 1.5 THEN '🟢 Near Target'
        ELSE '🔵 Low Inflation'
    END as "Inflation Status"
FROM inflation_fed
WHERE inflation IS NOT NULL
ORDER BY observation_date DESC
LIMIT 24;


-- ============================================
-- 4. LABOR MARKET HEALTH
-- Comprehensive employment picture
-- ============================================
WITH labor_metrics AS (
    SELECT 
        cm.observation_date,
        MAX(CASE WHEN cm.series_id = 'UNRATE' THEN cm.value END) as unemployment,
        MAX(CASE WHEN cm.series_id = 'UNRATE' THEN cm.yoy_change END) as unemp_change,
        MAX(CASE WHEN cm.series_id = 'ICSA' THEN cm.value END) as jobless_claims,
        MAX(CASE WHEN cm.series_id = 'PAYEMS' THEN cm.mom_change END) as payroll_change
    FROM calculated_metrics cm
    WHERE cm.observation_date >= '2020-01-01'
    GROUP BY cm.observation_date
)
SELECT 
    observation_date as "Date",
    ROUND(unemployment::numeric, 1) as "Unemployment %",
    ROUND(jobless_claims::numeric, 0) as "Weekly Claims",
    ROUND(payroll_change::numeric, 2) as "Payroll MoM %",
    CASE 
        WHEN unemployment < 4 AND jobless_claims < 250000 THEN '🟢 Strong'
        WHEN unemployment < 5 AND jobless_claims < 300000 THEN '🟡 Moderate'
        ELSE '🔴 Weak'
    END as "Labor Market"
FROM labor_metrics
WHERE unemployment IS NOT NULL
ORDER BY observation_date DESC
LIMIT 24;


-- ============================================
-- 5. CONSUMER HEALTH INDEX
-- Sentiment, spending, and housing
-- ============================================
WITH consumer_metrics AS (
    SELECT 
        cm.observation_date,
        MAX(CASE WHEN cm.series_id = 'UMCSENT' THEN cm.value END) as sentiment,
        MAX(CASE WHEN cm.series_id = 'UMCSENT' THEN cm.z_score END) as sentiment_zscore,
        MAX(CASE WHEN cm.series_id = 'RSXFS' THEN cm.yoy_change END) as retail_yoy,
        MAX(CASE WHEN cm.series_id = 'HOUST' THEN cm.value END) as housing_starts
    FROM calculated_metrics cm
    WHERE cm.observation_date >= '2020-01-01'
    GROUP BY cm.observation_date
)
SELECT 
    observation_date as "Date",
    ROUND(sentiment::numeric, 1) as "Consumer Sentiment",
    ROUND(retail_yoy::numeric, 2) as "Retail Sales YoY %",
    ROUND(housing_starts::numeric, 0) as "Housing Starts (K)",
    CASE 
        WHEN sentiment > 80 AND retail_yoy > 3 THEN '🟢 Confident'
        WHEN sentiment > 60 AND retail_yoy > 0 THEN '🟡 Cautious'
        ELSE '🔴 Pessimistic'
    END as "Consumer Health"
FROM consumer_metrics
WHERE sentiment IS NOT NULL
ORDER BY observation_date DESC
LIMIT 24;


-- ============================================
-- 6. ECONOMIC CYCLE DETECTION
-- Identifies where we are in the cycle
-- ============================================
WITH latest_indicators AS (
    SELECT 
        MAX(CASE WHEN series_id = 'GDP' THEN yoy_change END) as gdp_growth,
        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) as unemployment,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN yoy_change END) as inflation,
        MAX(CASE WHEN series_id = 'T10Y2Y' THEN value END) as yield_spread,
        MAX(CASE WHEN series_id = 'UMCSENT' THEN value END) as sentiment
    FROM calculated_metrics
    WHERE observation_date = (SELECT MAX(observation_date) FROM calculated_metrics WHERE series_id = 'UNRATE')
)
SELECT 
    CASE 
        WHEN gdp_growth > 2 AND unemployment < 4.5 AND inflation < 3 THEN 'EXPANSION'
        WHEN gdp_growth > 0 AND gdp_growth < 1.5 AND yield_spread < 0.5 THEN 'LATE CYCLE'
        WHEN gdp_growth < 0 OR unemployment > 6 THEN 'RECESSION'
        WHEN gdp_growth > 3 AND unemployment > 5 THEN 'EARLY RECOVERY'
        ELSE 'MID CYCLE'
    END as "Economic Cycle Phase",
    ROUND(gdp_growth::numeric, 2) as "GDP Growth %",
    ROUND(unemployment::numeric, 1) as "Unemployment %",
    ROUND(inflation::numeric, 2) as "Inflation %",
    ROUND(yield_spread::numeric, 2) as "Yield Spread",
    ROUND(sentiment::numeric, 1) as "Consumer Sentiment"
FROM latest_indicators;


-- ============================================
-- 7. CORRELATION MATRIX
-- Key relationships between indicators
-- ============================================
WITH paired_data AS (
    SELECT 
        cm.observation_date,
        MAX(CASE WHEN cm.series_id = 'UNRATE' THEN cm.value END) as unemployment,
        MAX(CASE WHEN cm.series_id = 'UMCSENT' THEN cm.value END) as sentiment,
        MAX(CASE WHEN cm.series_id = 'FEDFUNDS' THEN cm.value END) as fed_rate,
        MAX(CASE WHEN cm.series_id = 'CPIAUCSL' THEN cm.yoy_change END) as inflation,
        MAX(CASE WHEN cm.series_id = 'RSXFS' THEN cm.yoy_change END) as retail
    FROM calculated_metrics cm
    WHERE cm.observation_date >= '2010-01-01'
    GROUP BY cm.observation_date
    HAVING COUNT(DISTINCT cm.series_id) >= 4
)
SELECT 
    'Unemployment vs Sentiment' as "Relationship",
    ROUND(CORR(unemployment, sentiment)::numeric, 3) as "Correlation",
    CASE 
        WHEN CORR(unemployment, sentiment) < -0.5 THEN 'Strong Negative'
        WHEN CORR(unemployment, sentiment) < -0.2 THEN 'Moderate Negative'
        WHEN CORR(unemployment, sentiment) < 0.2 THEN 'Weak'
        ELSE 'Positive'
    END as "Strength"
FROM paired_data
UNION ALL
SELECT 
    'Fed Rate vs Inflation',
    ROUND(CORR(fed_rate, inflation)::numeric, 3),
    CASE WHEN CORR(fed_rate, inflation) > 0.5 THEN 'Strong Positive' ELSE 'Moderate' END
FROM paired_data
UNION ALL
SELECT 
    'Sentiment vs Retail Sales',
    ROUND(CORR(sentiment, retail)::numeric, 3),
    CASE WHEN CORR(sentiment, retail) > 0.5 THEN 'Strong Positive' ELSE 'Moderate' END
FROM paired_data;