-- =============================================================================
-- Silver Layer: Deduplicated, keyed tables via CDC
-- Merges exploded bronze events into SCD Type 1 tables keyed by date.
-- ${silver_schema} is resolved from the pipeline configuration at runtime.
-- =============================================================================

-- Target table for deduplicated daily impression counts
CREATE OR REFRESH STREAMING TABLE ${silver_schema}.impressions
  (date    DATE    COMMENT "Calendar date of the impression data point",
   count   BIGINT  COMMENT "Number of post impressions on this date")
COMMENT "Deduplicated daily post impression counts, keyed by date (SCD Type 1)";

-- CDC flow: upsert impression records by date, sequenced by source filename
CREATE FLOW impressions_cdc_flow
AS AUTO CDC INTO ${silver_schema}.impressions
FROM STREAM(impressions_exploded)
KEYS (date)
SEQUENCE BY _filename
COLUMNS * EXCEPT (_filename, _timestamp)
STORED AS SCD TYPE 1;

-- Target table for deduplicated daily comment counts
CREATE OR REFRESH STREAMING TABLE ${silver_schema}.comments
  (date    DATE    COMMENT "Calendar date of the comment data point",
   count   BIGINT  COMMENT "Number of post comments on this date")
COMMENT "Deduplicated daily post comment counts, keyed by date (SCD Type 1)";

-- CDC flow: upsert comment records by date, sequenced by source filename
CREATE FLOW comments_cdc_flow
AS AUTO CDC INTO ${silver_schema}.comments
FROM STREAM(comments_exploded)
KEYS (date)
SEQUENCE BY _filename
COLUMNS * EXCEPT (_filename, _timestamp)
STORED AS SCD TYPE 1;

-- Target table for deduplicated daily follower delta counts
CREATE OR REFRESH STREAMING TABLE ${silver_schema}.followers
  (date    DATE    COMMENT "Calendar date of the follower delta data point",
   count   BIGINT  COMMENT "Net change in followers on this date")
COMMENT "Deduplicated daily follower delta counts, keyed by date (SCD Type 1)";

-- CDC flow: upsert follower delta records by date, sequenced by source filename
CREATE FLOW followers_cdc_flow
AS AUTO CDC INTO ${silver_schema}.followers
FROM STREAM(followers_exploded)
KEYS (date)
SEQUENCE BY _filename
COLUMNS * EXCEPT (_filename, _timestamp)
STORED AS SCD TYPE 1;

-- Target table for deduplicated daily reaction counts
CREATE OR REFRESH STREAMING TABLE ${silver_schema}.reactions
  (date    DATE    COMMENT "Calendar date of the reaction data point",
   count   BIGINT  COMMENT "Number of post reactions on this date")
COMMENT "Deduplicated daily post reaction counts, keyed by date (SCD Type 1)";

-- CDC flow: upsert reaction records by date, sequenced by source filename
CREATE FLOW reactions_cdc_flow
AS AUTO CDC INTO ${silver_schema}.reactions
FROM STREAM(reactions_exploded)
KEYS (date)
SEQUENCE BY _filename
COLUMNS * EXCEPT (_filename, _timestamp)
STORED AS SCD TYPE 1;

-- Target table for deduplicated total follower count snapshots
CREATE OR REFRESH STREAMING TABLE ${silver_schema}.followers_agg
  (date    DATE    COMMENT "Calendar date of the total follower snapshot",
   count   BIGINT  COMMENT "Total follower count at the time of the snapshot")
COMMENT "Deduplicated total follower count snapshots, keyed by date (SCD Type 1)";

-- CDC flow: upsert total follower snapshots by date, sequenced by source filename
CREATE FLOW followers_agg_cdc_flow
AS AUTO CDC INTO ${silver_schema}.followers_agg
FROM STREAM(followers_agg_exploded)
KEYS (date)
SEQUENCE BY _filename
COLUMNS * EXCEPT (_filename, _timestamp)
STORED AS SCD TYPE 1;
