MERGE INTO tlc_trip_records.d_weather T
USING staging.weather S
ON T.date = S.date
WHEN NOT MATCHED THEN INSERT (date, temp, dewp, slp, stp, visib, wdsp, mxspd, gust, max, min, prcp, sndp, frshtt)
VALUES (DATE, TEMP, DEWP, SLP, STP, VISIB, WDSP, MXSPD, GUST, MAX, MIN, PRCP, SNDP, FRSHTT);