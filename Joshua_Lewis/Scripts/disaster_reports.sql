CREATE TABLE disaster_reports (
  report_id STRING,
  title STRING,
  created_date DATETIME,
  country STRING,
  source STRING,
  url STRING
);


--This is my ambee sql create statement desinged by CHatGPT
--based on the ambee api documentation
CREATE TABLE disaster_events (
  event_id STRING,              -- Unique identifier for each disaster event
  source_event_id STRING,       -- Original source's ID
  event_name STRING,            -- Name of the disaster event
  event_type STRING,            -- Type (e.g., SW = Severe Weather, WF = Wildfire, FL = Flood)
  date TIMESTAMP,               -- Reported start date/time of the event
  estimated_end_date TIMESTAMP, -- Expected end date (nullable)
  created_time TIMESTAMP,       -- Time this event was created in the system
  lat FLOAT64,                  -- Latitude
  lng FLOAT64,                  -- Longitude
  continent STRING              -- Region code (e.g., "nar" for North America)
);

