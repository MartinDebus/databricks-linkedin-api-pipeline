-- =============================================================================
-- Gold Layer: Aggregated reporting view
-- Joins all silver metrics into a single daily summary.
-- ${silver_schema} and ${gold_schema} are resolved from the pipeline config at runtime.
-- =============================================================================

-- Daily aggregated LinkedIn metrics across all signal types.
-- The spine is a UNION of all dates across all tables so that no date is lost
-- even if one metric has no data for a given day (e.g. no follower delta recorded).
-- All metric joins are LEFT JOINs — missing values appear as NULL.
CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.daily_metrics
  (date                DATE    COMMENT "Calendar date of the metrics",
   followers_count     BIGINT  COMMENT "Net change in followers on this date",
   followers_agg_count BIGINT  COMMENT "Total follower count snapshot on this date",
   impressions_count   BIGINT  COMMENT "Number of post impressions on this date",
   reactions_count     BIGINT  COMMENT "Number of post reactions on this date",
   comments_count      BIGINT  COMMENT "Number of post comments on this date")
COMMENT "LinkedIn daily metrics combining followers, impressions, reactions, and comments"
AS
WITH all_dates AS (
    -- Union of all dates across every metric table to form a complete spine
    SELECT date FROM ${silver_schema}.followers
    UNION
    SELECT date FROM ${silver_schema}.followers_agg
    UNION
    SELECT date FROM ${silver_schema}.impressions
    UNION
    SELECT date FROM ${silver_schema}.reactions
    UNION
    SELECT date FROM ${silver_schema}.comments
)
SELECT
    all_dates.date                  AS date,
    followers.count                 AS followers_count,
    followers_agg.count             AS followers_agg_count,
    impressions.count               AS impressions_count,
    reactions.count                 AS reactions_count,
    comments.count                  AS comments_count
FROM
    all_dates
LEFT JOIN
    ${silver_schema}.followers AS followers
    ON all_dates.date = followers.date
LEFT JOIN
    ${silver_schema}.followers_agg AS followers_agg
    ON all_dates.date = followers_agg.date
LEFT JOIN
    ${silver_schema}.impressions AS impressions
    ON all_dates.date = impressions.date
LEFT JOIN
    ${silver_schema}.reactions AS reactions
    ON all_dates.date = reactions.date
LEFT JOIN
    ${silver_schema}.comments AS comments
    ON all_dates.date = comments.date;
