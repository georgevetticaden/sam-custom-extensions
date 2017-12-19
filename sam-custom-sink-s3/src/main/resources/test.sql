-- Create a table for calculating Driver Average Speed
CREATE OR REPLACE STREAM "DRIVER_AVG_SPEED" (
   "driverId" INTEGER,
   "driverName" VARCHAR(500),
   "route" VARCHAR(500),
   "speedAvg" DOUBLE
  );

-- Create a pump that that calculates the average speed of driver over 1 minute period
CREATE OR REPLACE PUMP "DRIVER_AVG_SPEED_PUMP" AS 
INSERT INTO "DRIVER_AVG_SPEED" (
                          "driverId", 
                          "driverName",
                          "route",
                          "speedAvg"
                          ) 
SELECT STREAM "driverId", 
              "driverName",
              "route",
              AVG("speed")
FROM "SOURCE_SQL_STREAM_001" 
GROUP BY "driverId","driverName","route",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' MINUTE);
    
-- Create a table for speeding drivers
CREATE OR REPLACE STREAM "HIGH_SPEEDING_DRIVER" (
   "driverId" INTEGER,
   "driverName" VARCHAR(500),
   "route" VARCHAR(500),
   "speedAvg" DOUBLE
  );
  
--  Create a pump to detects drivers speeding over 80 in last minute
CREATE OR REPLACE PUMP "HIGH_SPEEDING_DRIVER_PUMP" AS 
INSERT INTO "HIGH_SPEEDING_DRIVER" (
                          "driverId", 
                          "driverName",
                          "route",
                          "speedAvg"
                          ) 
SELECT STREAM "driverId", 
              "driverName",
              "route",
              "speedAvg"
FROM "DRIVER_AVG_SPEED" where "speedAvg" > 80;


