-- =============================================================================
-- Bronze Layer: Raw ingestion from landing volume
-- Reads JSON files written by the ingest job and appends ingestion metadata.
-- ${volume} is resolved from the pipeline configuration at runtime.
-- =============================================================================

-- Raw post impression events per file, one row per source file
CREATE OR REFRESH STREAMING TABLE impressions
COMMENT "Raw post impression data ingested from the LinkedIn API landing volume"
AS SELECT
    *,
    CURRENT_TIMESTAMP() AS _timestamp,
    _metadata.file_path AS _filename
FROM
    STREAM read_files(
        "${volume}/impressions/",
        format => "json"
    );

-- Exploded impression events: one row per daily data point
-- Filters out API error responses where elements field is absent
CREATE OR REFRESH STREAMING TABLE impressions_exploded
COMMENT "Exploded impression events with one row per daily data point"
AS SELECT
    MAKE_DATE(element.dateRange.start.year, element.dateRange.start.month, element.dateRange.start.day) AS date,
    element.count AS count,
    _timestamp,
    _filename
FROM (
    SELECT
        EXPLODE(elements) AS element,
        _timestamp,
        _filename
    FROM STREAM(impressions)
    WHERE elements IS NOT NULL  -- skip API error responses (no elements field)
);

-- Raw comment events per file, one row per source file
CREATE OR REFRESH STREAMING TABLE comments
COMMENT "Raw post comment data ingested from the LinkedIn API landing volume"
AS SELECT
    *,
    CURRENT_TIMESTAMP() AS _timestamp,
    _metadata.file_path AS _filename
FROM
    STREAM read_files(
        "${volume}/comments/",
        format => "json"
    );

-- Exploded comment events: one row per daily data point
-- Filters out API error responses where elements field is absent
CREATE OR REFRESH STREAMING TABLE comments_exploded
COMMENT "Exploded comment events with one row per daily data point"
AS SELECT
    MAKE_DATE(element.dateRange.start.year, element.dateRange.start.month, element.dateRange.start.day) AS date,
    element.count AS count,
    _timestamp,
    _filename
FROM (
    SELECT
        EXPLODE(elements) AS element,
        _timestamp,
        _filename
    FROM STREAM(comments)
    WHERE elements IS NOT NULL  -- skip API error responses (no elements field)
);

-- Raw follower delta events per file, one row per source file
CREATE OR REFRESH STREAMING TABLE followers
COMMENT "Raw follower delta data ingested from the LinkedIn API landing volume"
AS SELECT
    *,
    CURRENT_TIMESTAMP() AS _timestamp,
    _metadata.file_path AS _filename
FROM
    STREAM read_files(
        "${volume}/followers/",
        format => "json"
    );

-- Exploded follower delta events: one row per daily data point
-- Filters out API error responses where elements field is absent
CREATE OR REFRESH STREAMING TABLE followers_exploded
COMMENT "Exploded follower delta events with one row per daily data point"
AS SELECT
    MAKE_DATE(element.dateRange.start.year, element.dateRange.start.month, element.dateRange.start.day) AS date,
    element.memberFollowersCount AS count,
    _timestamp,
    _filename
FROM (
    SELECT
        EXPLODE(elements) AS element,
        _timestamp,
        _filename
    FROM STREAM(followers)
    WHERE elements IS NOT NULL  -- skip API error responses (no elements field)
);

-- Raw total follower count snapshots per file (q=me endpoint)
CREATE OR REFRESH STREAMING TABLE followers_agg
COMMENT "Raw total follower count snapshots ingested from the LinkedIn API landing volume"
AS SELECT
    *,
    CURRENT_TIMESTAMP() AS _timestamp,
    _metadata.file_path AS _filename
FROM
    STREAM read_files(
        "${volume}/followers_agg/",
        format => "json"
    );

-- Exploded total follower snapshots: one row per snapshot
-- Date is derived from the ingest file timestamp (no dateRange in this endpoint)
-- Filters out API error responses where elements field is absent
CREATE OR REFRESH STREAMING TABLE followers_agg_exploded
COMMENT "Exploded total follower snapshots with date derived from the source filename timestamp"
AS SELECT
    TO_DATE(file_datetime) AS date,
    element.memberFollowersCount AS count,
    _timestamp,
    _filename
FROM (
    SELECT
        EXPLODE(elements) AS element,
        -- Extract the 14-digit timestamp from the filename (yyyyMMddHHmmss)
        to_timestamp(
            regexp_extract(_filename, r'(\d{14})\.json$', 1),
            'yyyyMMddHHmmss'
        ) AS file_datetime,
        _timestamp,
        _filename
    FROM STREAM(followers_agg)
    WHERE elements IS NOT NULL  -- skip API error responses (no elements field)
);

-- Raw reaction events per file, one row per source file
CREATE OR REFRESH STREAMING TABLE reactions
COMMENT "Raw post reaction data ingested from the LinkedIn API landing volume"
AS SELECT
    *,
    CURRENT_TIMESTAMP() AS _timestamp,
    _metadata.file_path AS _filename
FROM
    STREAM read_files(
        "${volume}/reactions/",
        format => "json"
    );

-- Exploded reaction events: one row per daily data point
-- Filters out API error responses where elements field is absent
CREATE OR REFRESH STREAMING TABLE reactions_exploded
COMMENT "Exploded reaction events with one row per daily data point"
AS SELECT
    MAKE_DATE(element.dateRange.start.year, element.dateRange.start.month, element.dateRange.start.day) AS date,
    element.count AS count,
    _timestamp,
    _filename
FROM (
    SELECT
        EXPLODE(elements) AS element,
        _timestamp,
        _filename
    FROM STREAM(reactions)
    WHERE elements IS NOT NULL  -- skip API error responses (no elements field)
);
