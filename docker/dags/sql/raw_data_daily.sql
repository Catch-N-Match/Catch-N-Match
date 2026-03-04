WITH raw AS (
SELECT
  -- event
  event_name,
  event_date,
  event_timestamp,
  event_value_in_usd,
  event_bundle_sequence_id,

  -- data stream
  stream_id,
  platform,

  -- user
  user_pseudo_id,
  user_first_touch_timestamp,
  user_ltv.revenue,
  user_ltv.currency,

  -- device
  device.category,
  device.mobile_brand_name,
  device.mobile_model_name,
  device.operating_system,
  device.operating_system_version,
  device.`language`,
  device.web_info.browser,
  device.web_info.browser_version,

  -- geo
  geo.continent,
  geo.sub_continent,
  geo.country,
  geo.region,
  geo.city,

  -- traffic
  traffic_source.medium,
  traffic_source.name,
  traffic_source.source,

  -- ecommerce
  ecommerce.total_item_quantity,
  ecommerce.purchase_revenue_in_usd,
  ecommerce.purchase_revenue,
  ecommerce.tax_value_in_usd,
  ecommerce.tax_value,
  ecommerce.unique_items,
  ecommerce.transaction_id,

  -- items
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_variant,
  i.item_category,
  i.price,
  i.price_in_usd,
  i.quantity,
  i.item_revenue,
  i.item_revenue_in_usd,
  i.item_list_id,
  i.item_list_index,
  i.promotion_name,
  i.creative_name,

  -- string_value only 18개
  MAX(IF(ep.key = 'campaign',        ep.value.string_value, NULL)) AS param_campaign,
  MAX(IF(ep.key = 'clean_event',     ep.value.string_value, NULL)) AS param_clean_event,
  MAX(IF(ep.key = 'coupon',          ep.value.string_value, NULL)) AS param_coupon,
  MAX(IF(ep.key = 'currency',        ep.value.string_value, NULL)) AS param_currency,
  MAX(IF(ep.key = 'link_classes',    ep.value.string_value, NULL)) AS param_link_classes,
  MAX(IF(ep.key = 'link_domain',     ep.value.string_value, NULL)) AS param_link_domain,
  MAX(IF(ep.key = 'link_url',        ep.value.string_value, NULL)) AS param_link_url,
  MAX(IF(ep.key = 'medium',          ep.value.string_value, NULL)) AS param_medium,
  MAX(IF(ep.key = 'outbound',        ep.value.string_value, NULL)) AS param_outbound,
  MAX(IF(ep.key = 'page_location',   ep.value.string_value, NULL)) AS param_page_location,
  MAX(IF(ep.key = 'page_referrer',   ep.value.string_value, NULL)) AS param_page_referrer,
  MAX(IF(ep.key = 'page_title',      ep.value.string_value, NULL)) AS param_page_title,
  MAX(IF(ep.key = 'payment_type',    ep.value.string_value, NULL)) AS param_payment_type,
  MAX(IF(ep.key = 'promotion_name',  ep.value.string_value, NULL)) AS param_promotion_name,
  MAX(IF(ep.key = 'search_term',     ep.value.string_value, NULL)) AS param_search_term,
  MAX(IF(ep.key = 'shipping_tier',   ep.value.string_value, NULL)) AS param_shipping_tier,
  MAX(IF(ep.key = 'source',          ep.value.string_value, NULL)) AS param_source,
  MAX(IF(ep.key = 'term',            ep.value.string_value, NULL)) AS param_term,

  -- int_value only 8개
  MAX(IF(ep.key = 'debug_mode',            ep.value.int_value, NULL)) AS param_debug_mode,
  MAX(IF(ep.key = 'engaged_session_event', ep.value.int_value, NULL)) AS param_engaged_session_event,
  MAX(IF(ep.key = 'engagement_time_msec',  ep.value.int_value, NULL)) AS param_engagement_time_msec,
  MAX(IF(ep.key = 'entrances',             ep.value.int_value, NULL)) AS param_entrances,
  MAX(IF(ep.key = 'ga_session_id',         ep.value.int_value, NULL)) AS param_ga_session_id,
  MAX(IF(ep.key = 'ga_session_number',     ep.value.int_value, NULL)) AS param_ga_session_number,
  MAX(IF(ep.key = 'percent_scrolled',      ep.value.int_value, NULL)) AS param_percent_scrolled,
  MAX(IF(ep.key = 'unique_search_term',    ep.value.int_value, NULL)) AS param_unique_search_term,

  -- 혼합 타입 (COALESCE 처리) 4개
  MAX(IF(ep.key = 'session_engaged',
    COALESCE(ep.value.string_value, CAST(ep.value.int_value AS STRING)), NULL)) AS param_session_engaged,
  MAX(IF(ep.key = 'tax',
    CAST(COALESCE(ep.value.double_value, CAST(ep.value.int_value AS FLOAT64)) AS STRING), NULL)) AS param_tax,
  MAX(IF(ep.key = 'transaction_id',
    COALESCE(ep.value.string_value, CAST(ep.value.int_value AS STRING)), NULL)) AS param_transaction_id,
  MAX(IF(ep.key = 'value',
    CAST(COALESCE(ep.value.double_value, CAST(ep.value.int_value AS FLOAT64)) AS STRING), NULL)) AS param_value,

  -- timezone offset 추가 (분 단위) -> GMT, hour_gmt 계산
  CASE
    -- 미국 (region 기준)
    WHEN geo.country = 'United States' THEN
      CASE geo.region
        WHEN 'California'           THEN -480
        WHEN 'Washington'           THEN -480
        WHEN 'Oregon'               THEN -480
        WHEN 'Nevada'               THEN -480
        WHEN 'Alaska'               THEN -540
        WHEN 'Hawaii'               THEN -600
        WHEN 'Colorado'             THEN -420
        WHEN 'Arizona'              THEN -420
        WHEN 'Utah'                 THEN -420
        WHEN 'Idaho'                THEN -420
        WHEN 'New Mexico'           THEN -420
        WHEN 'Wyoming'              THEN -420
        WHEN 'Montana'              THEN -420
        WHEN 'Texas'                THEN -360
        WHEN 'Illinois'             THEN -360
        WHEN 'Tennessee'            THEN -360
        WHEN 'Missouri'             THEN -360
        WHEN 'Wisconsin'            THEN -360
        WHEN 'Minnesota'            THEN -360
        WHEN 'Iowa'                 THEN -360
        WHEN 'Kansas'               THEN -360
        WHEN 'Louisiana'            THEN -360
        WHEN 'Nebraska'             THEN -360
        WHEN 'Oklahoma'             THEN -360
        WHEN 'Mississippi'          THEN -360
        WHEN 'Arkansas'             THEN -360
        WHEN 'Alabama'              THEN -360
        WHEN 'North Dakota'         THEN -360
        WHEN 'South Dakota'         THEN -360
        WHEN 'New York'             THEN -300
        WHEN 'Virginia'             THEN -300
        WHEN 'Florida'              THEN -300
        WHEN 'Massachusetts'        THEN -300
        WHEN 'Pennsylvania'         THEN -300
        WHEN 'New Jersey'           THEN -300
        WHEN 'Georgia'              THEN -300
        WHEN 'North Carolina'       THEN -300
        WHEN 'Ohio'                 THEN -300
        WHEN 'Michigan'             THEN -300
        WHEN 'Maryland'             THEN -300
        WHEN 'Indiana'              THEN -300
        WHEN 'Connecticut'          THEN -300
        WHEN 'South Carolina'       THEN -300
        WHEN 'District of Columbia' THEN -300
        WHEN 'Maine'                THEN -300
        WHEN 'West Virginia'        THEN -300
        WHEN 'Delaware'             THEN -300
        WHEN 'Rhode Island'         THEN -300
        WHEN 'Vermont'              THEN -300
        WHEN 'New Hampshire'        THEN -300
        WHEN 'Kentucky'             THEN -300
        ELSE -300
      END

    -- 캐나다 (region 기준)
    WHEN geo.country = 'Canada' THEN
      CASE geo.region
        WHEN 'British Columbia'          THEN -480
        WHEN 'Alberta'                   THEN -420
        WHEN 'Saskatchewan'              THEN -360
        WHEN 'Manitoba'                  THEN -360
        WHEN 'Ontario'                   THEN -300
        WHEN 'Quebec'                    THEN -300
        WHEN 'Nova Scotia'               THEN -240
        WHEN 'New Brunswick'             THEN -240
        WHEN 'Prince Edward Island'      THEN -240
        WHEN 'Newfoundland and Labrador' THEN -210
        ELSE -300
      END

    -- 러시아 (region 기준)
    WHEN geo.country = 'Russia' THEN
      CASE geo.region
        WHEN 'Moscow'            THEN 180
        WHEN 'Moscow Oblast'     THEN 180
        WHEN 'Krasnodar Krai'    THEN 180
        WHEN 'Saint Petersburg'  THEN 180
        WHEN 'Sverdlovsk Oblast' THEN 300
        ELSE 180
      END

    -- 호주 (region 기준)
    WHEN geo.country = 'Australia' THEN
      CASE geo.region
        WHEN 'New South Wales'              THEN 600
        WHEN 'Victoria'                     THEN 600
        WHEN 'Queensland'                   THEN 600
        WHEN 'Australian Capital Territory' THEN 600
        WHEN 'South Australia'              THEN 570
        WHEN 'Western Australia'            THEN 480
        ELSE 600
      END

    -- 브라질
    WHEN geo.country = 'Brazil' THEN -180

    -- 멕시코 (region 기준)
    WHEN geo.country = 'Mexico' THEN
      CASE geo.region
        WHEN 'Baja California'   THEN -480
        WHEN 'Sonora'            THEN -420
        WHEN 'Chihuahua'         THEN -420
        WHEN 'Quintana Roo'      THEN -300
        WHEN 'Mexico City'       THEN -360
        WHEN 'Jalisco'           THEN -360
        WHEN 'Tamaulipas'        THEN -360
        WHEN 'Puebla'            THEN -360
        WHEN 'Veracruz'          THEN -360
        WHEN 'Queretaro'         THEN -360
        WHEN 'Guanajuato'        THEN -360
        ELSE -360
      END

    -- 인도네시아 (region 기준)
    WHEN geo.country = 'Indonesia' THEN
      CASE geo.region
        WHEN 'Jakarta'                      THEN 420
        WHEN 'West Java'                    THEN 420
        WHEN 'East Java'                    THEN 420
        WHEN 'Banten'                       THEN 420
        WHEN 'Central Java'                 THEN 420
        WHEN 'Special Region of Yogyakarta' THEN 420
        WHEN 'North Sumatra'                THEN 420
        WHEN 'Riau Islands'                 THEN 420
        WHEN 'South Sulawesi'               THEN 480
        ELSE 420
      END

    -- 스페인
    WHEN geo.country = 'Spain' THEN
      CASE geo.region
        WHEN 'Canary Islands' THEN 0
        ELSE 60
      END

    -- 아메리카
    WHEN geo.country = 'Colombia'           THEN -300
    WHEN geo.country = 'Peru'               THEN -300
    WHEN geo.country = 'Ecuador'            THEN -300
    WHEN geo.country = 'Chile'              THEN -240
    WHEN geo.country = 'Argentina'          THEN -180
    WHEN geo.country = 'Uruguay'            THEN -180
    WHEN geo.country = 'Venezuela'          THEN -240
    WHEN geo.country = 'Bolivia'            THEN -240
    WHEN geo.country = 'Paraguay'           THEN -180
    WHEN geo.country = 'Guatemala'          THEN -360
    WHEN geo.country = 'El Salvador'        THEN -360
    WHEN geo.country = 'Costa Rica'         THEN -360
    WHEN geo.country = 'Honduras'           THEN -360
    WHEN geo.country = 'Panama'             THEN -300
    WHEN geo.country = 'Jamaica'            THEN -300
    WHEN geo.country = 'Dominican Republic' THEN -240
    WHEN geo.country = 'Trinidad & Tobago'  THEN -240
    WHEN geo.country = 'Puerto Rico'        THEN -240
    WHEN geo.country = 'Bahamas'            THEN -300
    -- 유럽
    WHEN geo.country = 'United Kingdom'     THEN 0
    WHEN geo.country = 'Ireland'            THEN 0
    WHEN geo.country = 'Portugal'           THEN 0
    WHEN geo.country = 'Iceland'            THEN 0
    WHEN geo.country = 'Germany'            THEN 60
    WHEN geo.country = 'France'             THEN 60
    WHEN geo.country = 'Italy'              THEN 60
    WHEN geo.country = 'Netherlands'        THEN 60
    WHEN geo.country = 'Belgium'            THEN 60
    WHEN geo.country = 'Switzerland'        THEN 60
    WHEN geo.country = 'Austria'            THEN 60
    WHEN geo.country = 'Sweden'             THEN 60
    WHEN geo.country = 'Norway'             THEN 60
    WHEN geo.country = 'Denmark'            THEN 60
    WHEN geo.country = 'Poland'             THEN 60
    WHEN geo.country = 'Czechia'            THEN 60
    WHEN geo.country = 'Hungary'            THEN 60
    WHEN geo.country = 'Croatia'            THEN 60
    WHEN geo.country = 'Serbia'             THEN 60
    WHEN geo.country = 'Slovakia'           THEN 60
    WHEN geo.country = 'Slovenia'           THEN 60
    WHEN geo.country = 'Bosnia & Herzegovina' THEN 60
    WHEN geo.country = 'North Macedonia'    THEN 60
    WHEN geo.country = 'Albania'            THEN 60
    WHEN geo.country = 'Kosovo'             THEN 60
    WHEN geo.country = 'Malta'              THEN 60
    WHEN geo.country = 'Luxembourg'         THEN 60
    WHEN geo.country = 'Finland'            THEN 120
    WHEN geo.country = 'Greece'             THEN 120
    WHEN geo.country = 'Romania'            THEN 120
    WHEN geo.country = 'Bulgaria'           THEN 120
    WHEN geo.country = 'Ukraine'            THEN 120
    WHEN geo.country = 'Lithuania'          THEN 120
    WHEN geo.country = 'Latvia'             THEN 120
    WHEN geo.country = 'Estonia'            THEN 120
    WHEN geo.country = 'Cyprus'             THEN 120
    WHEN geo.country = 'Belarus'            THEN 180
    WHEN geo.country = 'Georgia'            THEN 240
    -- 중동/아프리카
    WHEN geo.country = 'Turkey'             THEN 180
    WHEN geo.country = 'Israel'             THEN 120
    WHEN geo.country = 'Palestine'          THEN 120
    WHEN geo.country = 'Jordan'             THEN 180
    WHEN geo.country = 'Lebanon'            THEN 120
    WHEN geo.country = 'Iraq'               THEN 180
    WHEN geo.country = 'Bahrain'            THEN 180
    WHEN geo.country = 'Saudi Arabia'       THEN 180
    WHEN geo.country = 'Kuwait'             THEN 180
    WHEN geo.country = 'Qatar'              THEN 180
    WHEN geo.country = 'Oman'               THEN 240
    WHEN geo.country = 'United Arab Emirates' THEN 240
    WHEN geo.country = 'Armenia'            THEN 240
    WHEN geo.country = 'Azerbaijan'         THEN 240
    WHEN geo.country = 'Egypt'              THEN 120
    WHEN geo.country = 'Nigeria'            THEN 60
    WHEN geo.country = 'Kenya'              THEN 180
    WHEN geo.country = 'Ethiopia'           THEN 180
    WHEN geo.country = 'South Africa'       THEN 120
    WHEN geo.country = 'Ghana'              THEN 0
    WHEN geo.country = 'Algeria'            THEN 60
    WHEN geo.country = 'Morocco'            THEN 60
    WHEN geo.country = 'Tunisia'            THEN 60
    -- 아시아
    WHEN geo.country = 'Pakistan'           THEN 300
    WHEN geo.country = 'India'              THEN 330
    WHEN geo.country = 'Sri Lanka'          THEN 330
    WHEN geo.country = 'Nepal'              THEN 345
    WHEN geo.country = 'Bangladesh'         THEN 360
    WHEN geo.country = 'Kazakhstan'         THEN 360
    WHEN geo.country = 'Myanmar (Burma)'    THEN 390
    WHEN geo.country = 'Thailand'           THEN 420
    WHEN geo.country = 'Vietnam'            THEN 420
    WHEN geo.country = 'Cambodia'           THEN 420
    WHEN geo.country = 'Laos'               THEN 420
    WHEN geo.country = 'Mongolia'           THEN 480
    WHEN geo.country = 'China'              THEN 480
    WHEN geo.country = 'Singapore'          THEN 480
    WHEN geo.country = 'Malaysia'           THEN 480
    WHEN geo.country = 'Philippines'        THEN 480
    WHEN geo.country = 'Taiwan'             THEN 480
    WHEN geo.country = 'Hong Kong'          THEN 480
    WHEN geo.country = 'Macao'              THEN 480
    WHEN geo.country = 'South Korea'        THEN 540
    WHEN geo.country = 'Japan'              THEN 540
    WHEN geo.country = 'New Zealand'        THEN 720
    -- 오세아니아
    WHEN geo.country = 'Papua New Guinea'   THEN 600
    ELSE 0
  END AS offset_minutes

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
LEFT JOIN UNNEST(event_params) AS ep
LEFT JOIN UNNEST(items) AS i
WHERE _TABLE_SUFFIX = '{{ ti.xcom_pull(task_ids="compute_ga4_date") }}'
GROUP BY ALL
)

SELECT
  *,

  -- GMT 컬럼: UTC±HH:MM 형식
  CASE
    WHEN offset_minutes >= 0 THEN CONCAT(
      'UTC+',
      LPAD(CAST(DIV(offset_minutes, 60) AS STRING), 2, '0'), ':',
      LPAD(CAST(MOD(offset_minutes, 60) AS STRING), 2, '0')
    )
    ELSE CONCAT(
      'UTC-',
      LPAD(CAST(DIV(ABS(offset_minutes), 60) AS STRING), 2, '0'), ':',
      LPAD(CAST(MOD(ABS(offset_minutes), 60) AS STRING), 2, '0')
    )
  END AS GMT,

  -- hour_gmt 컬럼: 현지 시간 기준 시(0~23)
  EXTRACT(HOUR FROM
    TIMESTAMP_ADD(
      TIMESTAMP_MICROS(event_timestamp),
      INTERVAL offset_minutes MINUTE
    )
  ) AS hour_gmt

FROM raw
