
-- Athena Tables with Partition Projection 


-- For the partition projection we use "enum" projection. 
-- With enum, you give Athena the full list of possible values up front, so it can enumerate them itself. 
-- That means you can query without filtering on postal_code, and
--   aggregate across all of them — which is what a dashboard needs.

 
CREATE DATABASE IF NOT EXISTS mobile_network_aggregations;
 

DROP TABLE IF EXISTS mobile_network_aggregations.signal_strength_by_operator;
DROP TABLE IF EXISTS mobile_network_aggregations.gps_precision_by_provider;
DROP TABLE IF EXISTS mobile_network_aggregations.status_count;
 
 

-- TABLE 1 — signal_strength_by_operator.
-- partition_hour: integer 0-23 (zero-padded to 2 digits)
-- postal_code:    enum (full list of known values)

CREATE EXTERNAL TABLE mobile_network_aggregations.signal_strength_by_operator (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    operator        STRING,
    average_signal  DOUBLE
)
PARTITIONED BY (
    partition_hour  STRING,
    postal_code     STRING
)
STORED AS PARQUET
LOCATION 's3://roy-projects/mobile-network-lab/aggregations/signal_strength_by_operator/'
TBLPROPERTIES (
    'projection.enabled'               = 'true',
    'projection.partition_hour.type'   = 'integer',
    'projection.partition_hour.range'  = '0,23',
    'projection.partition_hour.digits' = '2',
    'projection.postal_code.type'      = 'enum',
    'projection.postal_code.values'    = '00100,01000,10100,10400,20100,30100,30200,30500,40100,40200,50100,60200,60300,70100,80100,80200,90100',
    'storage.location.template'        = 's3://roy-projects/mobile-network-lab/aggregations/signal_strength_by_operator/partition_hour=${partition_hour}/postal_code=${postal_code}/'
);
 
 

-- TABLE 2 — gps_precision_by_provider
-- partition_hour: integer 0-23
-- provider:       enum (only two values: gps, fused)

CREATE EXTERNAL TABLE mobile_network_aggregations.gps_precision_by_provider (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    average_precision   DOUBLE,
    average_satellites  DOUBLE
)
PARTITIONED BY (
    partition_hour  STRING,
    provider        STRING
)
STORED AS PARQUET
LOCATION 's3://roy-projects/mobile-network-lab/aggregations/gps_precision_by_provider/'
TBLPROPERTIES (
    'projection.enabled'               = 'true',
    'projection.partition_hour.type'   = 'integer',
    'projection.partition_hour.range'  = '0,23',
    'projection.partition_hour.digits' = '2',
    'projection.provider.type'         = 'enum',
    'projection.provider.values'       = 'gps,fused',
    'storage.location.template'        = 's3://roy-projects/mobile-network-lab/aggregations/gps_precision_by_provider/partition_hour=${partition_hour}/provider=${provider}/'
);
 
 
-- TABLE 3 — status_count
-- partition_hour: integer 0-23
-- postal_code:    enum (same list as table 1)

CREATE EXTERNAL TABLE mobile_network_aggregations.status_count (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    status          STRING,
    status_count    BIGINT
)
PARTITIONED BY (
    partition_hour  STRING,
    postal_code     STRING
)
STORED AS PARQUET
LOCATION 's3://roy-projects/mobile-network-lab/aggregations/status_count/'
TBLPROPERTIES (
    'projection.enabled'               = 'true',
    'projection.partition_hour.type'   = 'integer',
    'projection.partition_hour.range'  = '0,23',
    'projection.partition_hour.digits' = '2',
    'projection.postal_code.type'      = 'enum',
    'projection.postal_code.values'    = '00100,01000,10100,10400,20100,30100,30200,30500,40100,40200,50100,60200,60300,70100,80100,80200,90100',
    'storage.location.template'        = 's3://roy-projects/mobile-network-lab/aggregations/status_count/partition_hour=${partition_hour}/postal_code=${postal_code}/'
);
 
 

-- SAMPLE ATHENA QUERIES 


-- Q1: Average signal by Operator, aggregating across all postal codes
SELECT operator,
       ROUND(average_signal, 1) AS avg_signal,
       window_start,
       window_end
FROM mobile_network_aggregations.signal_strength_by_operator
WHERE partition_hour = '04'
ORDER BY window_start DESC
LIMIT 20;
 
-- Q2: average signal per operator across a whole hour (cross-postal aggregation)
SELECT operator,
       ROUND(AVG(average_signal), 1) AS hourly_avg_signal,
       COUNT(*)                      AS window_count
FROM mobile_network_aggregations.signal_strength_by_operator
WHERE partition_hour = '04'
GROUP BY operator
ORDER BY hourly_avg_signal DESC;
 
-- Q3: GPS precision by provider for one hour
SELECT provider,
       ROUND(average_precision, 2)  AS avg_precision_m,
       ROUND(average_satellites, 1) AS avg_satellites
FROM mobile_network_aggregations.gps_precision_by_provider
WHERE partition_hour = '04'
GROUP BY provider, average_precision, average_satellites;
 
-- Q4: status counts for one hour, all postal codes
SELECT postal_code, status, SUM(status_count) AS total
FROM mobile_network_aggregations.status_count
WHERE partition_hour = '04'
GROUP BY postal_code, status
ORDER BY total DESC
LIMIT 20;
 
 