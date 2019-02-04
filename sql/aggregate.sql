WITH schedule AS
(
	SELECT DISTINCT
		CAST(CAST(b.start_scheduled_arrival_time_converted AS DATETIME) AS TIME) [start_schedule_leave],
		CAST(CAST(b.stop_scheduled_arrival_time_converted AS DATETIME) AS TIME) [end_schedule_leave],
		CAST(CAST(b.stop_predicted_arrival_time_converted AS DATETIME) AS TIME) [end_actual_leave],
		b.route_id,
		b.route_direction,
		CASE
			WHEN dd.Day_Of_Week = 1 THEN 'Sunday'
			WHEN dd.Day_Of_Week = 7 THEN 'Saturday'
			ELSE 'Weekday'
		END [DOW],
		dd.Day_Of_Week
	FROM dbo.[BUSTed-Routes] b
		LEFT JOIN dbo.DimDates dd
			ON dd.PK_Date = CAST(b.start_scheduled_arrival_time_converted AS DATE)
),

leave_by AS
(
	SELECT 
		IIF(CAST(a.actual_avg AS decimal) = 0.0, NULL, CAST(DATEADD(SECOND, CAST(a.actual_avg AS decimal) * -60, CAST(a.time_1 AS DATETIME)) AS TIME)) [DepartBy],
		a.*
	FROM dbo.Data a
)
,

schedule_grab AS
(
	SELECT
		*,
		IIF(a.DepartBy > a.time_1, 1, 0) [grab]
	FROM leave_by a
)
,

sugg_sched AS (

SELECT 
	*,
	IIF(b.start_schedule_leave IS NULL, (SELECT TOP 1
		start_schedule_leave
	FROM schedule a
	WHERE a.route_id = b.route_id
		AND IIF(b.DOW = 'Sunday', 'Saturday', IIF(b.DOW = 'Saturday', 'Weekday', 'Weekday')) = a.DOW
		AND a.Route_direction = b.route_direction
		AND ((b.time_3 <= 100 AND a.start_schedule_leave >= b.DepartBy) OR (b.time_3 > 100 AND a.start_schedule_leave <= b.DepartBy))
	ORDER BY a.start_schedule_leave DESC
	), b.start_schedule_leave) [ssl]
FROM
(
	SELECT
		*
	FROM
	(
		SELECT 
			a.*,
			IIF(CAST(scheduled_avg AS DECIMAL) = 0.0, NULL, b.start_schedule_leave) [start_schedule_leave],
			ROW_NUMBER() OVER(PARTITION BY a.route_id, a.time_1, a.DOW, a.route_direction, a.departby ORDER BY b.start_schedule_leave DESC) [Row]
		FROM schedule_grab a
			LEFT JOIN schedule b 
				ON a.route_id = b.route_id
				AND a.DOW = b.DOW
				AND a.Route_direction = b.route_direction
				AND b.start_schedule_leave <= a.DepartBy
		WHERE (a.grab = 0  OR (a.grab = 1 AND a.DOW = 'Weekday'))
	) AS a
	WHERE a.Row = 1
) AS b

UNION ALL

SELECT
	*,
	a.start_schedule_leave
FROM
(
	SELECT 
		a.*,
		IIF(CAST(scheduled_avg AS DECIMAL) = 0.0, NULL, b.start_schedule_leave) [start_schedule_leave],
		ROW_NUMBER() OVER(PARTITION BY a.route_id, a.time_1, a.route_direction, a.departby ORDER BY b.start_schedule_leave DESC) [Row]
	FROM schedule_grab a
		LEFT JOIN schedule b
			ON a.route_id = b.route_id
			AND b.DOW = 'Weekday'
			AND a.Route_direction = b.route_direction
			AND b.start_schedule_leave <= a.DepartBy
	WHERE a.grab = 1 AND a.DOW = 'Saturday'  --nd CAST(scheduled_avg AS DECIMAL) > 0.0
) AS a
WHERE a.Row = 1

UNION ALL

SELECT
	*,
	a.start_schedule_leave
FROM
(
	SELECT 
		a.*,
		IIF(CAST(scheduled_avg AS DECIMAL) = 0.0, NULL, b.start_schedule_leave) [start_schedule_leave],
		ROW_NUMBER() OVER(PARTITION BY a.route_id, a.time_1, a.route_direction, a.departby ORDER BY b.start_schedule_leave DESC) [Row]
	FROM schedule_grab a
		LEFT JOIN schedule b
			ON a.route_id = b.route_id
			AND b.DOW = 'Saturday'
			AND a.Route_direction = b.route_direction
			AND b.start_schedule_leave <= a.DepartBy
	WHERE a.grab = 1 AND a.DOW = 'Sunday' --and CAST(scheduled_avg AS DECIMAL) > 0.0
) AS a
WHERE a.Row = 1

)
,


fd AS
(
SELECT 
	*,
	ABS(IIF(b.late = 1, DATEDIFF(SECOND, CAST(b.Time_1 AS DATETIME), CAST(b.end_actual_leave AS DATETIME))/60.0, 0)) [minutes_late]
FROM
(

SELECT 
	a.*,
	b.end_actual_leave,
	CASE
		WHEN CAST(a.Time_1 AS TIME) IN ('0:00:00', '0:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '23:%' THEN 0
		WHEN CAST(a.Time_1 AS TIME) IN ('23:00:00', '23:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '0:%' THEN 1
		WHEN CAST(a.Time_1 AS TIME) < CAST(b.end_actual_leave AS TIME) THEN 1
		ELSE 0
	END [late]
FROM sugg_sched a
	LEFT JOIN schedule b
		ON	a.route_id = b.route_id
		AND IIF(a.start_schedule_leave IS NULL, IIF(a.DOW = 'Sunday', 'Saturday', IIF(a.DOW = 'Saturday', 'Weekday', 'Weekday')), a.DOW) = b.DOW
		AND a.route_direction = b.route_direction
		AND a.ssl = b.start_schedule_leave
WHERE a.grab = 0  OR (a.grab = 1 AND a.DOW = 'Weekday') 

UNION ALL

SELECT 
	a.*,
	b.end_actual_leave,
	CASE
		WHEN CAST(a.Time_1 AS TIME) IN ('0:00:00', '0:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '23:%' THEN 0
		WHEN CAST(a.Time_1 AS TIME) IN ('23:00:00', '23:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '0:%' THEN 1
		WHEN CAST(a.Time_1 AS TIME) < CAST(b.end_actual_leave AS TIME) THEN 1
		ELSE 0
	END [late]
FROM sugg_sched a
	LEFT JOIN schedule b
		ON	a.route_id = b.route_id
		AND b.DOW = 'Weekday'
		AND a.route_direction = b.route_direction
		AND a.start_schedule_leave = b.start_schedule_leave
WHERE a.grab = 1 AND a.DOW = 'Saturday'

UNION ALL

SELECT 
	a.*,
	b.end_actual_leave,
	CASE
		WHEN CAST(a.Time_1 AS TIME) IN ('0:00:00', '0:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '23:%' THEN 0
		WHEN CAST(a.Time_1 AS TIME) IN ('23:00:00', '23:30:00') AND CAST(b.end_actual_leave AS TIME) LIKE '0:%' THEN 1
		WHEN CAST(a.Time_1 AS TIME) < CAST(b.end_actual_leave AS TIME) THEN 1
		ELSE 0
	END [late]
FROM sugg_sched a
	LEFT JOIN schedule b
		ON	a.route_id = b.route_id
		AND b.DOW = 'Saturday'
		AND a.route_direction = b.route_direction
		AND a.start_schedule_leave = b.start_schedule_leave
WHERE a.grab = 1 AND a.DOW = 'Sunday'
) AS B
)


SELECT
	*,
	IIF(totalLates is null or totaldata is null or totaldata =0, null, CAST(ISNULL(totalLates,0) AS decimal)/ISNULL(totaldata,1)) [Percent]
FROM
(
SELECT
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl [start_schedule_leave],
	SUM(IIF(a.minutes_late > 0 AND a.minutes_late < 5, 0 , late)) [TotalLates],
	COUNT(a.ssl) [TotalData],
	'(0-5)' [RISK]
FROM fd a
GROUP BY
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl

UNION ALL

SELECT
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl,
	SUM(IIF(a.minutes_late >= 5 AND a.minutes_late < 10, 0 , late)) [TotalLates],
	COUNT(a.ssl) [TotalData],
	'[5-10)' [RISK]
FROM fd a
GROUP BY
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl

UNION ALL

SELECT
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl,
	SUM(IIF(a.minutes_late >= 10, 0 , late)) [TotalLates],
	COUNT(a.ssl) [TotalData],
	'10+' [RISK]
FROM fd a
GROUP BY
	a.DepartBy,
	a.time_1,
	a.time_2, 
	a.time_3,
	a.DOW,
	a.route_id,
	a.route_direction,
	a.scheduled_avg,
	a.actual_avg,
	a.trip_headsign,
	a.ssl

--UNION ALL

--SELECT
--	a.DepartBy,
--	a.time_1,
--	a.time_2, 
--	a.time_3,
--	a.DOW,
--	a.route_id,
--	a.route_direction,
--	a.scheduled_avg,
--	a.actual_avg,
--	a.trip_headsign,
--	a.ssl,
--	SUM(IIF(a.minutes_late < 15, 0 , late)) [TotalLates],
--	COUNT(a.ssl) [TotalData],
--	15 [RISK]
--FROM fd a
--GROUP BY
--	a.DepartBy,
--	a.time_1,
--	a.time_2, 
--	a.time_3,
--	a.DOW,
--	a.route_id,
--	a.route_direction,
--	a.scheduled_avg,
--	a.actual_avg,
--	a.trip_headsign,
--	a.ssl

--UNION ALL

--SELECT
--	a.DepartBy,
--	a.time_1,
--	a.time_2, 
--	a.time_3,
--	a.DOW,
--	a.route_id,
--	a.route_direction,
--	a.scheduled_avg,
--	a.actual_avg,
--	a.trip_headsign,
--	a.ssl,
--	SUM(IIF(a.minutes_late < 20, 0 , late)) [TotalLates],
--	COUNT(a.ssl) [TotalData],
--	20 [RISK]
--FROM fd a
--GROUP BY
--	a.DepartBy,
--	a.time_1,
--	a.time_2, 
--	a.time_3,
--	a.DOW,
--	a.route_id,
--	a.route_direction,
--	a.scheduled_avg,
--	a.actual_avg,
--	a.trip_headsign,
--	a.ssl
) AS a