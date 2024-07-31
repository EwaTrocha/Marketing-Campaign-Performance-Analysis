WITH event_data AS (
  SELECT
    event_date,
    user_pseudo_id AS user_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_time,
    CASE 
      WHEN campaign IN ('<Other>', '(data deleted)') THEN 'other'
      ELSE campaign 
    END AS campaign,
  -- getting event before timestamp
    LAG(TIMESTAMP_MICROS(event_timestamp)) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS previous_event_time

  FROM 
    `tc-da-1.turing_data_analytics.raw_events`
  WHERE 
    event_timestamp IS NOT NULL
    AND campaign IS NOT NULL

  ORDER BY event_date, user_id
),

session_identification AS (
SELECT
  event_date,
  user_id,
  event_time,
  previous_event_time,
--indentify events separated by more than 30min (1800sec)
  IF(TIMESTAMP_DIFF(event_time, previous_event_time, SECOND) > 1800, 1, 0) AS is_new_session,
  campaign
FROM 
  event_data
ORDER BY event_date, user_id
),

session_data AS (
  SELECT *,
  -- sum all '1' to give each session id if they come form the same user, day
    SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM 
    session_identification
  ORDER BY event_date, user_id
),

session_start_end AS (
  SELECT 
    PARSE_DATE('%Y%m%d', event_date) AS date_format,
    user_id,
  -- getting session start time by session_id
    MIN(event_time) OVER (PARTITION BY user_id, session_id) AS session_start,
  -- getting session end by session_id
    MAX(event_time) OVER (PARTITION BY user_id, session_id) AS session_end,
    campaign,
  -- adding row number to each event by session and event time
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS row_num
  FROM
    session_data
  ORDER BY user_id, session_start, event_time
)

SELECT DISTINCT *,
  TIMESTAMP_DIFF(session_end, session_start, SECOND) AS session_duration

FROM session_start_end

WHERE row_num = 1

--all other campaigns
AND (campaign = ('other') OR campaign = ('(organic)') OR campaign = ('(referral)') OR campaign = ('(direct)'))

--all  campaigns
--AND campaign != ('other') AND campaign != ('(organic)') AND campaign != ('(referral)') AND campaign != ('(direct)')

ORDER BY date_format, user_id, session_start ASC;