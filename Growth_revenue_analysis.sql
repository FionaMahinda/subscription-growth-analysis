-- create database (run as admin if needed)
CREATE DATABASE subscription_growth;

-- inside the database:
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_subscriptions (
  User_ID                BIGINT,
  Signup_Date            DATE,
  Lead_Source            TEXT,
  Plan_Type              TEXT,
  Monthly_Active_Days    INT,
  Feature_Usage_Score    NUMERIC,
  Customer_Support_Tickets        INT,
  Monthly_Revenue        NUMERIC,
  Churn_Risk_Score       NUMERIC 
);

SELECT column_name, data_type 
FROM information_schema.columns
WHERE table_schema = 'analytics'
AND table_name = 'raw_subscriptions';

DROP TABLE IF EXISTS analytics.raw_subscriptions;

CREATE TABLE analytics.raw_subscriptions (
	User_ID       TEXT,
	Signup_Date   TEXT,
	Lead_Source   TEXT,
	Plan_Type     TEXT,
	Monthly_Active_Days  INT,
	Feature_Usage_Score   INT,
	Customer_Support_Tickets  INT,
	Monthly_Revenue  NUMERIC,
	Churn_Risk_Score  NUMERIC
	
);

COPY analytics.raw_subscriptions(User_ID,Signup_Date,Lead_Source,Plan_Type,Monthly_Active_Days,Feature_Usage_Score,
	Customer_Support_Tickets,Monthly_Revenue,Churn_Risk_Score )
FROM 'C:\Users\growth_analysis_data.csv'
DELIMITER ','
CSV HEADER;

--Row count --
SELECT COUNT(*) AS total_users
FROM analytics.raw_subscriptions;


-- looking for null values

SELECT
  SUM(CASE WHEN User_Id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
  SUM(CASE WHEN Signup_Date IS NULL THEN 1 ELSE 0 END) AS null_signup_date,
  SUM(CASE WHEN Lead_Source IS NULL OR lead_source = '' THEN 1 ELSE 0 END) AS null_lead_source,
  SUM(CASE WHEN Plan_Type IS NULL OR plan_type = '' THEN 1 ELSE 0 END) AS null_plan_type
FROM analytics.raw_subscriptions;

-- looking for outlier values
SELECT
  MIN(Monthly_Active_Days) AS min_active_days,
  MAX(Monthly_Active_Days) AS max_active_days,
  MIN(Feature_Usage_Score) AS min_feature_usage,
  MAX(Feature_Usage_Score) AS max_feature_usage,
  MIN(Customer_Support_Tickets) AS min_tickets,
  MAX(Customer_Support_Tickets) AS max_tickets,
  MIN(Monthly_Revenue) AS min_revenue,
  MAX(Monthly_Revenue) AS max_revenue,
  MIN(Churn_Risk_score) AS min_churn_risk,
  MAX(Churn_Risk_Score) AS max_churn_risk
FROM analytics.raw_subscriptions;


DROP TABLE IF EXISTS analytics.subscriptions_clean;

CREATE TABLE analytics.subscriptions_clean AS
SELECT
  user_id,
  Signup_Date,
  LOWER(TRIM(Lead_Source)) AS lead_source,
  LOWER(TRIM(Plan_Type))   AS plan_type,

  -- Keep numeric fields, but guard against NULLs
  COALESCE(Monthly_Active_Days, 0) AS monthly_active_days,
  COALESCE(Feature_Usage_Score, 0) AS feature_usage_score,
  COALESCE(Customer_Support_Tickets, 0) AS customer_support_tickets,
  COALESCE(Monthly_Revenue, 0) AS monthly_revenue,
  COALESCE(Churn_Risk_Score, 0) AS churn_risk_score,

  -- 1) Simple churn-risk bucket (tune thresholds if you want)
  CASE
    WHEN COALESCE(Churn_Risk_Score, 0) >= 0.60 THEN 'high'
    WHEN COALESCE(Churn_Risk_Score, 0) >= 0.30 THEN 'medium'
    ELSE 'low'
  END AS churn_risk_bucket,

  -- 2) Engagement bucket (based on active days)
  CASE
    WHEN COALESCE(Monthly_Active_Days, 0) >= 20 THEN 'high'
    WHEN COALESCE(Monthly_Active_Days, 0) >= 10 THEN 'medium'
    ELSE 'low'
  END AS engagement_bucket,

  -- 3) Friction bucket (support tickets)
  CASE
    WHEN COALESCE(Customer_Support_Tickets, 0) >= 2 THEN 'high'
    WHEN COALESCE(Customer_Support_Tickets, 0) = 1 THEN 'medium'
    ELSE 'low'
  END AS friction_bucket,

  -- 4) Paying vs non-paying
  CASE
    WHEN COALESCE(Monthly_Revenue, 0) > 0 THEN 'paying'
    ELSE 'non_paying'
  END AS revenue_status,

  -- 5) Signup cohort (month)
      Signup_Date AS signup_month
FROM analytics.raw_subscriptions;

SELECT
  Plan_Type,
  Lead_Source,
  Churn_Risk_Score,

  COUNT(*) AS users,
  AVG(Monthly_Active_Days) AS avg_active_days,
  AVG(Feature_Usage_Score) AS avg_feature_usage,
  AVG(Customer_Support_Tickets) AS avg_tickets,
  AVG(Monthly_Revenue) AS avg_monthly_revenue,
  SUM(Monthly_Revenue) AS total_monthly_revenue
FROM analytics.raw_subscriptions
GROUP BY 1,2,3
ORDER BY total_monthly_revenue DESC, users DESC;

--Does friction(customer_support_tickets) correlate with churn risk?
SELECT
  Customer_Support_Tickets,
  Churn_Risk_Score,
  COUNT(*) AS users,
  TRUNC(AVG(Customer_Support_Tickets)::numeric, 2) AS avg_tickets,
  TRUNC(AVG(Monthly_Revenue)::numeric,2) AS avg_revenue
FROM analytics.raw_subscriptions
GROUP BY 1,2
ORDER BY Customer_Support_Tickets, Churn_Risk_Score;

--WHICH CHANNELS BRING "GOOD USERS"

SELECT
  Lead_Source,
  COUNT(*) AS users,
  TRUNC(AVG(Churn_Risk_Score)::numeric,2) AS avg_churn_risk,
  TRUNC(AVG(Monthly_Active_Days)::numeric,2) AS avg_active_days,
  TRUNC(AVG(Monthly_Revenue)::numeric,2) AS avg_revenue,
  SUM(Monthly_Revenue) AS total_revenue
FROM analytics.raw_subscriptions
GROUP BY 1
ORDER BY total_revenue DESC;

-- Does engagement predict revenue + lower risk?

SELECT
  Feature_Usage_Score,
  
  COUNT(*) AS users,
  TRUNC(AVG(Monthly_Revenue)::numeric,2) AS avg_rev,
  TRUNC(AVG(Churn_Risk_Score)::numeric,2) AS avg_risk,
  TRUNC(AVG(Feature_Usage_Score)::numeric,2) AS avg_usage,
  TRUNC(AVG(Customer_Support_Tickets)::numeric,2) AS avg_tickets
FROM analytics.raw_subscriptions
GROUP BY 1
ORDER BY Feature_Usage_Score DESC;
   

-- Cohort × Acquisition Channel (growth insight gold)

SELECT
  
  Lead_Source,
  COUNT(*) AS users,
  TRUNC(AVG(Feature_Usage_Score):: numeric,2) AS avg_feature_usage,
  TRUNC(AVG(Churn_Risk_Score)::numeric,2) AS avg_churn_risk
FROM analytics.raw_subscriptions
GROUP BY Lead_Source
ORDER BY avg_churn_risk DESC;

-- PART 1 — Churn bottleneck analysis
-- Goal: Understand why your highest-revenue segment is churning.

WITH paying_users AS (
  SELECT *
  FROM analytics.raw_subscriptions
  WHERE Monthly_Revenue > 0
)
SELECT
  Plan_Type,
  COUNT(*) AS users,
  TRUNC(AVG(Monthly_Revenue)::numeric, 2) AS avg_revenue,
  TRUNC(AVG(Churn_Risk_Score)::numeric, 2) AS avg_churn_risk,
  TRUNC(AVG(Feature_Usage_Score)::numeric, 1) AS avg_feature_usage,
  TRUNC(AVG(Monthly_Active_Days)::numeric, 1) AS avg_active_days,
  TRUNC(AVG(Customer_Support_Tickets)::numeric, 1) AS avg_support_tickets
FROM paying_users
GROUP BY Plan_Type
ORDER BY avg_churn_risk DESC;

--Engagement vs churn (critical insight)
--Now we test the core hypothesis:
--High churn is driven by low engagement, even among paying users.

SELECT
  CASE
    WHEN Feature_Usage_Score < 50 THEN 'Low engagement'
    WHEN Feature_Usage_Score < 80 THEN 'Medium engagement'
    ELSE 'High engagement'
  END AS engagement_band,

  COUNT(*) AS users,
  TRUNC(AVG(Monthly_Revenue)::numeric, 2) AS avg_revenue,
  TRUNC(AVG(Churn_Risk_Score)::numeric, 2) AS avg_churn_risk
FROM analytics.raw_subscriptions
WHERE Monthly_Revenue > 0
GROUP BY engagement_band
ORDER BY avg_churn_risk DESC;

--PART 2 — Quantify revenue at risk
--Goal: Turn churn into a money problem, not a “metric”.

-- Monthly revenue at risk

SELECT
  COUNT(*) AS high_risk_users,
  TRUNC(SUM(Monthly_Revenue)::numeric, 2) AS revenue_at_risk
FROM analytics.raw_subscriptions
WHERE Churn_Risk_Score > 0.6;


-- Revenue at risk by segment (where to act first)

SELECT
  Plan_Type,
  COUNT(*) AS users,
  TRUNC(SUM(Monthly_Revenue)::numeric, 2) AS revenue_at_risk
FROM analytics.raw_subscriptions
WHERE Churn_Risk_Score > 0.6
GROUP BY Plan_Type
ORDER BY revenue_at_risk DESC;

-- A. Confirm which segment is “high-value”

SELECT
  Lead_Source,
  Plan_Type,
  COUNT(*) AS users,
  TRUNC(AVG(Monthly_Revenue)::numeric, 2) AS avg_revenue,
  TRUNC(AVG(Churn_Risk_Score)::numeric, 2) AS avg_churn_risk
FROM analytics.raw_subscriptions
WHERE Monthly_Revenue > 0
GROUP BY Lead_Source, Plan_Type
ORDER BY avg_revenue DESC, avg_churn_risk DESC;

-- Revenue-at-risk quantification

SELECT
  COUNT(*) AS high_risk_users,
  TRUNC(SUM(Monthly_Revenue)::numeric, 2) AS monthly_revenue_at_risk
FROM analytics.raw_subscriptions
WHERE Churn_Risk_Score >= 0.60
  AND monthly_revenue > 0;

