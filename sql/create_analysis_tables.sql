-- SQL script to create BigQuery tables for storing AI analysis results
-- Project: company-wide-370010
-- Dataset: 1_Youtube_Metrics_Dump

-- ============================================================================
-- Table 1: AI_Analysis_Results (Script Quality Analysis)
-- ============================================================================
CREATE TABLE IF NOT EXISTS `company-wide-370010.1_Youtube_Metrics_Dump.AI_Analysis_Results` (
  video_id STRING NOT NULL,
  channel_code STRING,
  analysis_timestamp TIMESTAMP NOT NULL,
  script_quality_score FLOAT64,
  hook_effectiveness_score FLOAT64,
  call_to_action_score FLOAT64,
  persuasion_effectiveness_score FLOAT64,
  user_intent_match_score FLOAT64,
  persuasion_techniques ARRAY<STRING>,
  key_strengths ARRAY<STRING>,
  improvement_areas ARRAY<STRING>,
  target_audience STRING,
  content_value_score FLOAT64,
  identified_intent STRING,
  has_clear_intro BOOL,
  has_clear_cta BOOL,
  problem_solution_structure BOOL,
  readability_score FLOAT64
)
PARTITION BY DATE(analysis_timestamp)
CLUSTER BY video_id, channel_code
OPTIONS(
  description="AI-powered script quality analysis results for YouTube videos",
  labels=[("source", "claude_api"), ("purpose", "analytics")]
);

-- ============================================================================
-- Table 2: AI_Affiliate_Recommendations (Product Recommendations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS `company-wide-370010.1_Youtube_Metrics_Dump.AI_Affiliate_Recommendations` (
  video_id STRING NOT NULL,
  recommendation_timestamp TIMESTAMP NOT NULL,
  product_rank INT64,
  product_name STRING,
  product_category STRING,
  relevance_score FLOAT64,
  conversion_probability FLOAT64,
  recommendation_reasoning STRING,
  where_to_mention STRING,
  mentioned_in_video BOOL,
  amazon_asin STRING,
  price_range STRING
)
PARTITION BY DATE(recommendation_timestamp)
CLUSTER BY video_id, product_rank
OPTIONS(
  description="AI-generated affiliate product recommendations for YouTube videos",
  labels=[("source", "claude_api"), ("purpose", "affiliate_marketing")]
);

-- ============================================================================
-- Table 3: AI_Description_Analysis (Description CTR Analysis)
-- ============================================================================
CREATE TABLE IF NOT EXISTS `company-wide-370010.1_Youtube_Metrics_Dump.AI_Description_Analysis` (
  video_id STRING NOT NULL,
  analysis_timestamp TIMESTAMP NOT NULL,
  cta_effectiveness_score FLOAT64,
  description_quality_score FLOAT64,
  seo_score FLOAT64,
  total_links INT64,
  affiliate_links INT64,
  link_positioning_score FLOAT64,
  has_clear_cta BOOL,
  optimization_suggestions ARRAY<STRING>,
  missing_elements ARRAY<STRING>,
  strengths ARRAY<STRING>
)
PARTITION BY DATE(analysis_timestamp)
CLUSTER BY video_id
OPTIONS(
  description="AI-powered video description CTR and optimization analysis",
  labels=[("source", "claude_api"), ("purpose", "ctr_optimization")]
);

-- ============================================================================
-- Table 4: AI_Conversion_Analysis (Conversion Rate Drivers)
-- ============================================================================
CREATE TABLE IF NOT EXISTS `company-wide-370010.1_Youtube_Metrics_Dump.AI_Conversion_Analysis` (
  video_id STRING NOT NULL,
  analysis_timestamp TIMESTAMP NOT NULL,
  metrics_date DATE,
  revenue FLOAT64,
  clicks INT64,
  sales INT64,
  views INT64,
  conversion_rate FLOAT64,
  revenue_per_click FLOAT64,
  revenue_per_1k_views FLOAT64,
  conversion_drivers ARRAY<STRING>,
  underperformance_reasons ARRAY<STRING>,
  recommendations ARRAY<STRING>
)
PARTITION BY DATE(analysis_timestamp)
CLUSTER BY video_id
OPTIONS(
  description="AI-powered conversion rate and revenue driver analysis",
  labels=[("source", "claude_api"), ("purpose", "conversion_optimization")]
);

-- ============================================================================
-- Indexes (BigQuery doesn't support traditional indexes, clustering is used)
-- ============================================================================

-- Note: All tables are partitioned by analysis/recommendation timestamp for query performance
-- and clustered by video_id for efficient lookups by video

-- ============================================================================
-- Sample Query Examples
-- ============================================================================

-- Get latest script analysis for a video:
-- SELECT * FROM `company-wide-370010.1_Youtube_Metrics_Dump.AI_Analysis_Results`
-- WHERE video_id = 'your_video_id'
-- ORDER BY analysis_timestamp DESC
-- LIMIT 1;

-- Get all affiliate recommendations for a video:
-- SELECT * FROM `company-wide-370010.1_Youtube_Metrics_Dump.AI_Affiliate_Recommendations`
-- WHERE video_id = 'your_video_id'
-- ORDER BY recommendation_timestamp DESC, product_rank ASC;

-- Get videos with high script quality scores:
-- SELECT video_id, script_quality_score, analysis_timestamp
-- FROM `company-wide-370010.1_Youtube_Metrics_Dump.AI_Analysis_Results`
-- WHERE script_quality_score >= 8.0
-- ORDER BY analysis_timestamp DESC;
