-- ============================================================
-- Project 3: Reddit Sentiment Streaming Pipeline
-- SQL Queries — BigQuery Analytics
-- Author: Ahmad Zulham Hamdan
-- ============================================================


-- ── DDL: Raw sentiment table ──────────────────────────────────
CREATE TABLE IF NOT EXISTS `your_project.reddit_sentiment_analytics.post_sentiment`
(
  post_id              STRING     NOT NULL,
  title                STRING,
  body                 STRING,
  subreddit            STRING,
  author               STRING,
  score                INT64,
  upvote_ratio         FLOAT64,
  num_comments         INT64,
  flair                STRING,
  is_self              BOOL,
  created_utc          TIMESTAMP,
  ingested_at          TIMESTAMP,
  sentiment_label      STRING,    -- positive / negative / neutral
  sentiment_score      FLOAT64,   -- ensemble score -1.0 to 1.0
  vader_compound       FLOAT64,
  textblob_polarity    FLOAT64,
  textblob_subjectivity FLOAT64,
  source               STRING
)
PARTITION BY DATE(created_utc)
CLUSTER BY subreddit, sentiment_label
OPTIONS (
  description = 'Reddit posts with NLP sentiment scores',
  require_partition_filter = FALSE
);

-- ── DDL: Subreddit daily aggregate ───────────────────────────
CREATE TABLE IF NOT EXISTS `your_project.reddit_sentiment_analytics.subreddit_daily_agg`
(
  post_date       DATE,
  subreddit       STRING,
  total_posts     INT64,
  avg_sentiment   FLOAT64,
  positive_pct    FLOAT64,
  negative_count  INT64,
  positive_count  INT64,
  neutral_count   INT64,
  avg_reddit_score FLOAT64
)
PARTITION BY post_date
CLUSTER BY subreddit;


-- ── QUERY 1: Hourly sentiment trend (last 48 hours) ──────────
SELECT
  TIMESTAMP_TRUNC(created_utc, HOUR)            AS hour_bucket,
  subreddit,
  COUNT(*)                                       AS post_count,
  ROUND(AVG(sentiment_score), 4)                AS avg_sentiment,
  COUNTIF(sentiment_label = 'positive')          AS positive,
  COUNTIF(sentiment_label = 'negative')          AS negative,
  COUNTIF(sentiment_label = 'neutral')           AS neutral,
  ROUND(
    COUNTIF(sentiment_label = 'positive') / COUNT(*) * 100, 1
  )                                              AS positive_pct
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE created_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY 1, 2
ORDER BY hour_bucket DESC, subreddit;


-- ── QUERY 2: Top 20 most positive posts today ─────────────────
SELECT
  subreddit,
  title,
  score                                          AS reddit_score,
  ROUND(sentiment_score, 3)                     AS sentiment_score,
  num_comments,
  flair,
  created_utc
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE DATE(created_utc) = CURRENT_DATE()
  AND sentiment_label = 'positive'
  AND score >= 50
ORDER BY sentiment_score DESC
LIMIT 20;


-- ── QUERY 3: Sentiment distribution by subreddit ─────────────
SELECT
  subreddit,
  COUNT(*)                                       AS total_posts,
  ROUND(AVG(sentiment_score), 4)                AS avg_sentiment,
  ROUND(AVG(upvote_ratio) * 100, 1)             AS avg_upvote_pct,
  COUNTIF(sentiment_label = 'positive')          AS positive,
  COUNTIF(sentiment_label = 'negative')          AS negative,
  COUNTIF(sentiment_label = 'neutral')           AS neutral,
  ROUND(
    COUNTIF(sentiment_label = 'positive') / COUNT(*) * 100, 1
  )                                              AS positive_pct,
  ROUND(
    COUNTIF(sentiment_label = 'negative') / COUNT(*) * 100, 1
  )                                              AS negative_pct
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE created_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY subreddit
ORDER BY avg_sentiment DESC;


-- ── QUERY 4: Controversial posts (negative + high engagement) ─
SELECT
  subreddit,
  title,
  score,
  num_comments,
  ROUND(sentiment_score, 3)                     AS sentiment_score,
  ROUND(upvote_ratio * 100, 1)                  AS upvote_pct,
  created_utc
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE DATE(created_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND sentiment_label = 'negative'
  AND num_comments >= 50
  AND score >= 100
ORDER BY num_comments DESC
LIMIT 15;


-- ── QUERY 5: 7-day rolling average sentiment ─────────────────
WITH daily AS (
  SELECT
    DATE(created_utc)        AS post_date,
    subreddit,
    AVG(sentiment_score)     AS daily_avg
  FROM `your_project.reddit_sentiment_analytics.post_sentiment`
  WHERE created_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY 1, 2
)
SELECT
  post_date,
  subreddit,
  ROUND(daily_avg, 4)                           AS daily_avg,
  ROUND(AVG(daily_avg) OVER (
    PARTITION BY subreddit
    ORDER BY post_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 4)                                         AS rolling_7d_avg
FROM daily
ORDER BY subreddit, post_date DESC;


-- ── QUERY 6: Pipeline health check ───────────────────────────
SELECT
  'Last ingestion'                              AS metric,
  CAST(MAX(ingested_at) AS STRING)              AS value
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
UNION ALL
SELECT 'Posts last hour', CAST(COUNT(*) AS STRING)
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
UNION ALL
SELECT 'Total posts today', CAST(COUNT(*) AS STRING)
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE DATE(ingested_at) = CURRENT_DATE()
UNION ALL
SELECT 'Null sentiment labels', CAST(COUNTIF(sentiment_label IS NULL) AS STRING)
FROM `your_project.reddit_sentiment_analytics.post_sentiment`
WHERE DATE(ingested_at) = CURRENT_DATE();
