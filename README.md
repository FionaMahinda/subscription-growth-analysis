# Subscription Growth & Churn Risk Analysis (SQL)

## Overview
This project analyzes user engagement, churn risk, and revenue performance for a
subscription-based SaaS business using PostgreSQL.

The goal was to:
- Identify high-value but high-churn user segments
- Understand which acquisition channels drive quality users
- Quantify revenue at risk due to churn
- Translate churn metrics into clear business impact

## Dataset
The dataset simulates subscription user behavior, including:
- Signup date and acquisition channel
- Plan type and engagement metrics
- Feature usage and support tickets
- Monthly revenue and churn risk score

## Key Analyses
- Data cleaning and feature engineering
- Engagement, friction, and churn segmentation
- Acquisition channel quality analysis
- Churn bottleneck identification for paying users
- Revenue-at-risk quantification

## Key Insights
- Some high-revenue segments also showed the highest churn risk
- Feature engagement was a stronger churn predictor than plan price
- Certain acquisition channels delivered volume but lower-quality users
- A small group of high-risk users accounted for a disproportionate share of revenue at risk

## Tools
- PostgreSQL
- SQL (CTEs, aggregations, segmentation logic)



