-- ============================================
-- ECONOMIC INDICATORS DATABASE SCHEMA
-- Author: Osvaldo Ruiz
-- Description: Schema for FRED economic data
-- ============================================

-- Drop existing tables (for clean rebuild)
DROP TABLE IF EXISTS calculated_metrics CASCADE;
DROP TABLE IF EXISTS observations CASCADE;
DROP TABLE IF EXISTS series_metadata CASCADE;
DROP VIEW IF EXISTS economic_dashboard_view CASCADE;
DROP VIEW IF EXISTS current_snapshot_view CASCADE;

-- ============================================
-- TABLE: series_metadata
-- Stores information about each economic series
-- ============================================
CREATE TABLE series_metadata (
    series_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    frequency VARCHAR(20),
    units VARCHAR(100),
    seasonal_adjustment VARCHAR(50),
    category VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE series_metadata IS 'Metadata for each FRED economic indicator series';

-- ============================================
-- TABLE: observations
-- Raw data points from FRED API
-- ============================================
CREATE TABLE observations (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(50) REFERENCES series_metadata(series_id),
    observation_date DATE NOT NULL,
    value DECIMAL(15,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, observation_date)
);

COMMENT ON TABLE observations IS 'Raw economic data observations from FRED';

-- ============================================
-- TABLE: calculated_metrics
-- Derived analytics on top of raw data
-- ============================================
CREATE TABLE calculated_metrics (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(50) REFERENCES series_metadata(series_id),
    observation_date DATE NOT NULL,
    value DECIMAL(15,4),
    mom_change DECIMAL(10,4),           -- Month-over-month % change
    yoy_change DECIMAL(10,4),           -- Year-over-year % change
    rolling_avg_3m DECIMAL(15,4),       -- 3-month rolling average
    rolling_avg_12m DECIMAL(15,4),      -- 12-month rolling average
    z_score DECIMAL(10,4),              -- Standard deviations from mean
    percentile_rank DECIMAL(5,2),       -- Historical percentile
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, observation_date)
);

COMMENT ON TABLE calculated_metrics IS 'Calculated analytics and transformations';

-- ============================================
-- INDEXES for query performance
-- ============================================
CREATE INDEX idx_obs_series_date ON observations(series_id, observation_date);
CREATE INDEX idx_obs_date ON observations(observation_date);
CREATE INDEX idx_calc_series_date ON calculated_metrics(series_id, observation_date);
CREATE INDEX idx_calc_date ON calculated_metrics(observation_date);

-- ============================================
-- VIEW: economic_dashboard_view
-- Pivoted data for dashboard consumption
-- ============================================
CREATE VIEW economic_dashboard_view AS
SELECT 
    cm.observation_date,
    MAX(CASE WHEN cm.series_id = 'GDP' THEN cm.yoy_change END) as gdp_growth_yoy,
    MAX(CASE WHEN cm.series_id = 'UNRATE' THEN cm.value END) as unemployment_rate,
    MAX(CASE WHEN cm.series_id = 'CPIAUCSL' THEN cm.yoy_change END) as inflation_yoy,
    MAX(CASE WHEN cm.series_id = 'FEDFUNDS' THEN cm.value END) as fed_funds_rate,
    MAX(CASE WHEN cm.series_id = 'UMCSENT' THEN cm.value END) as consumer_sentiment,
    MAX(CASE WHEN cm.series_id = 'HOUST' THEN cm.value END) as housing_starts,
    MAX(CASE WHEN cm.series_id = 'RSXFS' THEN cm.yoy_change END) as retail_sales_yoy,
    MAX(CASE WHEN cm.series_id = 'INDPRO' THEN cm.yoy_change END) as industrial_prod_yoy,
    MAX(CASE WHEN cm.series_id = 'T10Y2Y' THEN cm.value END) as yield_curve_spread,
    MAX(CASE WHEN cm.series_id = 'ICSA' THEN cm.value END) as jobless_claims
FROM calculated_metrics cm
GROUP BY cm.observation_date
ORDER BY cm.observation_date;

-- ============================================
-- VIEW: current_snapshot_view
-- Latest values for each indicator
-- ============================================
CREATE VIEW current_snapshot_view AS
WITH latest AS (
    SELECT 
        series_id,
        value,
        mom_change,
        yoy_change,
        z_score,
        observation_date,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY observation_date DESC) as rn
    FROM calculated_metrics
)
SELECT 
    m.series_id,
    m.title,
    m.units,
    m.category,
    l.value as current_value,
    l.mom_change,
    l.yoy_change,
    l.z_score,
    l.observation_date as as_of_date,
    CASE 
        WHEN l.z_score > 1.5 THEN 'Significantly Above Normal'
        WHEN l.z_score > 0.5 THEN 'Above Normal'
        WHEN l.z_score < -1.5 THEN 'Significantly Below Normal'
        WHEN l.z_score < -0.5 THEN 'Below Normal'
        ELSE 'Normal Range'
    END as status
FROM latest l
JOIN series_metadata m ON l.series_id = m.series_id
WHERE l.rn = 1;

-- Verify creation
SELECT 'Schema created successfully' as status;