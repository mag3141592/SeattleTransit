DECLARE @Route NVARCHAR(10)
SET @Route = '1_100252'; --270


WITH stop_sequence AS 
(
	SELECT DISTINCT
		b.route_id,
		b.route,
		MIN(b.latitude) OVER(PARTITION BY b.route_id ORDER BY b.latitude ASC) [start_lat],
		MAX(b.latitude) OVER(PARTITION BY b.route_id ORDER BY b.latitude DESC) [end_lat]
	FROM
	(
		SELECT DISTINCT
			a.route_id,
			CASE 
				WHEN a.route_id = '1_100151' THEN '26'
				WHEN a.route_id = '1_100252' THEN '62'
				WHEN a.route_id = '1_100190' THEN '316'
				WHEN a.route_id = '1_100270' THEN '76'
			END [route],
			a.stop_id,
			a.stop_sequence,
			sl.latitude,
			a.trip_headsign
		FROM dbo.arrivals_departures_0728_0804 a
			JOIN dbo.green_lake_stop_locations sl
				ON sl.stops = a.stop_id
		WHERE 
			a.route_id = @Route
	) AS b
)
,

stop_agg as
(
	SELECT 
		c.*,
		CAST(c.scheduled_arrival_time_converted AS TIME) [scheduled_time],
		CAST(c.predicted_arrival_time_converted AS TIME) [predicted_time]
	FROM
	(
		SELECT 
			b.*,
			ROW_NUMBER() OVER (PARTITION BY b.stop_id, b.route_id, b.trip_headsign, b.scheduled_arrival_time ORDER BY ABS(b.number_of_stops_away) ASC, b.scheduled_arrival_time ASC) [row_id]
		FROM
		(
			SELECT 
				a.[current_time],
				a.trip_id, 
				a.trip_headsign,
				a.route_id,
				a.stop_id,
				a.stop_sequence,
				a.scheduled_arrival_time,
				a.scheduled_departure_time,
				a.predicted_arrival_time,
				a.predicted_departure_time,
				DATEADD(HOUR, -7, DATEADD(S, CAST(a.scheduled_arrival_time AS BIGINT)/1000, '1-1-1970')) [scheduled_arrival_time_converted],
				DATEADD(HOUR, -7, DATEADD(S, CAST(a.scheduled_departure_time AS BIGINT)/1000, '1-1-1970')) [scheduled_departure_time_converted],
				DATEADD(HOUR, -7, IIF(a.predicted_arrival_time = '0', DATEADD(S, CAST(a.scheduled_arrival_time AS BIGINT)/1000 + CAST(a.schedule_deviation AS INT), '1-1-1970'), DATEADD(S, CAST(a.predicted_arrival_time AS BIGINT)/1000, '1-1-1970'))) [predicted_arrival_time_converted],
				DATEADD(HOUR, -7, IIF(a.predicted_departure_time = '0', DATEADD(S, CAST(a.scheduled_departure_time AS BIGINT)/1000 + CAST(a.schedule_deviation AS INT), '1-1-1970'), DATEADD(S, CAST(a.predicted_departure_time AS BIGINT)/1000, '1-1-1970'))) [predicted_departure_time_converted],
				a.distance_from_stop,
				a.number_of_stops_away,
				a.schedule_deviation,
				a.distance_along_trip,
				CASE 
					WHEN a.route_id = '1_100151' THEN '26'
					WHEN a.route_id = '1_100252' THEN '62'
					WHEN a.route_id = '1_100190' THEN '316'
					WHEN a.route_id = '1_100270' THEN '76'
				END [route],
				CASE 
					WHEN a.stop_id = '1_26220' AND a.route_id = '1_100151' THEN 'Start - South' --26
					WHEN a.stop_id = '1_433' AND a.route_id = '1_100151' THEN 'Stop - South'
					WHEN a.stop_id = '1_570' AND a.route_id = '1_100151' THEN 'Start - North'
					WHEN a.stop_id = '1_27150' AND a.route_id = '1_100151' THEN 'Stop - North'

					WHEN a.stop_id = '1_36960' AND a.route_id = '1_100252' THEN 'Start - South' --62
					WHEN a.stop_id IN ('1_430', '1_590') AND a.route_id = '1_100252' THEN 'Stop - South'
					WHEN a.stop_id IN ('1_450', '1_570') AND a.route_id = '1_100252' THEN 'Start - North'
					WHEN a.stop_id = '1_16410' AND a.route_id = '1_100252' THEN 'Stop - North'

					WHEN a.stop_id = '1_16409' AND a.route_id = '1_100190' THEN 'Start - South' --316
					WHEN a.stop_id = '1_1215' AND a.route_id = '1_100190' THEN 'Stop - South'
					WHEN a.stop_id = '1_690' AND a.route_id = '1_100190' THEN 'Start - North'
					WHEN a.stop_id = '1_16509' AND a.route_id = '1_100190' THEN 'Stop - North'

					WHEN a.stop_id = '1_36960' AND a.route_id = '1_100270' THEN 'Start - South' --76
					WHEN a.stop_id = '1_1215' AND a.route_id = '1_100270' THEN 'Stop - South'
					WHEN a.stop_id = '1_690' AND a.route_id = '1_100270' THEN 'Start - North'
					WHEN a.stop_id = '1_16419' AND a.route_id = '1_100270' THEN 'Stop - North'
				END [route_end_points],
				b.latitude,
				b.longitude
			FROM dbo.arrivals_departures_0728_0804 a
				LEFT JOIN dbo.green_lake_stop_locations	b
					ON	b.stops = a.stop_id
			WHERE a.route_id = @Route
		) AS b
	) AS c
	WHERE
		c.row_id = 1
)
--,

--route_agg AS
--(
	Select distinct
		sa.*,
		ROW_NUMBER() OVER(PARTITION BY sa.route_id ORDER BY sa.route_id, sa.trip_headsign, RIGHT(sa.route_end_points, 5), sa.trip_id, sa.scheduled_arrival_time) [Row]
	FROM stop_sequence ss
		JOIN stop_agg sa
			ON  sa.route_id = ss.route_id
			AND sa.latitude BETWEEN ss.start_lat and ss.end_lat
	WHERE sa.route_end_points like 'Start%' OR sa.route_end_points LIKE 'Stop%'
--)
--,

--calculations AS
--(
--	SELECT
--		b.*,
--		CASE
--			WHEN DATEPART(dw, b.start_scheduled_arrival_time_converted) = 1 THEN 'Sunday'
--			WHEN DATEPART(dw, b.start_scheduled_arrival_time_converted) = 7 THEN 'Saturday'
--			ELSE 'Weekday'
--		END [DOW],
--		DATEPART(dw, b.start_scheduled_arrival_time_converted) [Day_of_week],
--		DATENAME(dw, b.start_scheduled_arrival_time_converted) [DWN]
--	FROM
--	(
--		SELECT 
--			a.route_id,
--			a.route,
--			a.trip_headsign,
--			a.trip_id [start_trip_id],
--			b.trip_id [stop_trip_id],
--			IIF(RIGHT(a.route_end_points, 5) = 'South', 0, 1) [route_direction],
--			a.stop_id [start_stop_id],
--			a.route_end_points [route_start],
--			b.stop_id [end_stop_id],
--			b.route_end_points [route_stop],
--			a.scheduled_arrival_time_converted [start_scheduled_arrival_time_converted],
--			a.predicted_arrival_time_converted [start_predicted_arrival_time_converted],
--			b.scheduled_arrival_time_converted [stop_scheduled_arrival_time_converted],
--			b.predicted_arrival_time_converted [stop_predicted_arrival_time_converted],
--			a.schedule_deviation [pickup_schedule_deviation_sec],
--			CAST(a.[schedule_deviation] AS DECIMAL)/60.0 [pickup_schedule_deviation_min],
--			b.schedule_deviation [dropoff_schedule_deviation_sec],
--			CAST(b.[schedule_deviation] AS DECIMAL)/60.0 [dropoff_schedule_deviation_min],
--			DATEDIFF(SECOND, a.predicted_arrival_time_converted, b.predicted_arrival_time_converted)/60.0 [actual_route_length_min],
--			DATEDIFF(SECOND, a.scheduled_arrival_time_converted, b.scheduled_arrival_time_converted)/60.0 [scheduled_route_length_min],
--			b.scheduled_time,
--			b.predicted_time
--		From route_agg a
--			Join route_agg b
--				ON a.row = b.row - 1
--				AND b.route_end_points like 'Stop%'
--				AND RIGHT(a.route_end_points, 5) = RIGHT(b.route_end_points, 5)
--				AND a.trip_headsign = b.trip_headsign
--				AND a.trip_id = b.trip_id
--		WHERE 
--			a.route_end_points like 'Start%'
--	) AS b
--	--ORDER BY 
--	--	b.route_direction, b.start_scheduled_arrival_time_converted
--)
--,

--base AS
--(
--	SELECT 
--		* 
--	FROM dbo.Time t
--		CROSS APPLY 
--			(SELECT DISTINCT
--				CASE
--					WHEN dd.Day_Of_Week = 1 THEN 'Sunday'
--					WHEN dd.Day_Of_Week = 7 THEN 'Saturday'
--					ELSE 'Weekday'
--				END [DOW],
--				dd.Day_Of_Week
--				FROM dbo.DimDates dd
--			) AS d
--		CROSS APPLY 
--			(SELECT DISTINCT
--				Route_ID
--				FROM dbo.arrivals_departures_0728_0804
--				where route_id = @Route
--			) AS a
--		CROSS APPLY 
--			(SELECT DISTINCT
--				route_direction
--				FROM calculations
--			) AS b
--)
--,

--avgs AS
--(
--	SELECT
--		a.[time],
--		a.converted,
--		a.DOW,
--		a.route_id,
--		a.route_direction,
--		a.trip_headsign,
--		AVG(a.Scheduled) [ScheduledAvg],
--		AVG(a.Actual) [ActualAvg]
--	FROM
--	(
--		SELECT
--			a.*,
--			c1.Day_Of_Week [1],
--			c1.DOW [2],
--			CAST(a.start_interval AS TIME) [Start],
--			c1.scheduled_time,
--			CAST(a.end_interval AS TIME) [Stop],
--			c1.route,
--			c1.trip_headsign,
--			c1.scheduled_route_length_min [Scheduled],
--			c1.actual_route_length_min [Actual]
--		FROM base a
--			JOIN calculations c1
--				ON	c1.Day_Of_Week = a.Day_Of_Week
--				AND c1.route_id = a.route_id
--				AND c1.route_direction = a.route_direction
--				AND c1.scheduled_time BETWEEN CAST(a.start_interval AS TIME) AND CAST(a.end_interval AS TIME)
--		WHERE a.converted > 30 and a.converted < 2300

--		UNION ALL

--		SELECT
--			a.*,
--			c1.Day_Of_Week,
--			c1.DOW,
--			CAST(a.start_interval AS TIME) [Start],
--			c1.scheduled_time,
--			CAST(a.end_interval AS TIME) [Stop],
--			c1.route,
--			c1.trip_headsign,
--			c1.scheduled_route_length_min [Scheduled],
--			c1.actual_route_length_min [Actual]
--		FROM base a
--			JOIN calculations c1
--				ON	c1.route_id = a.route_id
--				AND c1.route_direction = a.route_direction
--				AND ((c1.Day_Of_Week = a.Day_Of_Week - 1 AND c1.scheduled_time >= CAST(a.start_interval AS TIME)) 
--				OR (c1.Day_Of_Week = a.Day_Of_Week AND c1.scheduled_time <= CAST(a.end_interval AS TIME)))
--		WHERE a.converted <= 30

--		UNION ALL

--		SELECT
--			a.*,
--			c1.Day_Of_Week,
--			c1.DOW,
--			CAST(a.start_interval AS TIME) [Start],
--			c1.scheduled_time,
--			CAST(a.end_interval AS TIME) [Stop],
--			c1.route,
--			c1.trip_headsign,
--			c1.scheduled_route_length_min [Scheduled],
--			c1.actual_route_length_min [Actual]
--		FROM base a
--			JOIN calculations c1
--				ON	c1.route_id = a.route_id
--				AND c1.route_direction = a.route_direction
--				AND ((c1.Day_Of_Week = a.Day_Of_Week AND c1.scheduled_time >= CAST(a.start_interval AS TIME)) 
--				OR (c1.Day_Of_Week = a.Day_Of_Week + 1 AND c1.scheduled_time <= CAST(a.end_interval AS TIME)))
--		WHERE a.converted >= 2300
--	) AS a
--	GROUP BY
--		a.[time],
--		a.converted,
--		a.DOW,
--		a.route_id,
--		a.route_direction,
--		a.trip_headsign
--)

--SELECT DISTINCT
--	CAST(a.time AS TIME) [time_1],
--	a.time [time_2],
--	a.converted [time_3],
--	a.DOW,
--	a.route_id,
--	a.route_direction,
--	b.ScheduledAvg,
--	b.ActualAvg,
--	b.trip_headsign
--FROM BASE a
--	LEFT JOIN avgs b
--		ON a.route_id = b.route_id
--		AND a.DOW = b.DOW
--		AND a.[time] = b.[time]
--		AND a.route_direction = b.route_direction
--ORDER BY
--	a.route_id,
--	a.route_direction,
--	CAST(a.time AS TIME)

