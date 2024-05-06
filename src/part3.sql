--3.1
CREATE OR REPLACE FUNCTION fnc_transfferedpoints_readble() 
RETURNS TABLE (
    Peer1           VARCHAR,
    Peer2           VARCHAR,
    PointsAmount    INT
) AS 
$fnc_transfferedpoints_readble$
BEGIN
RETURN QUERY
	SELECT tp.CheckingPeer, tp.CheckedPeer, COALESCE(tp.PointsAmount - tp2.PointsAmount, tp.PointsAmount)
	FROM TransferredPoints tp
	LEFT JOIN TransferredPoints tp2 ON tp2.CheckingPeer = tp.CheckedPeer AND tp2.CheckedPeer = tp.CheckingPeer;
END;
$fnc_transfferedpoints_readble$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_transfferedpoints_readble();

--3.2
CREATE OR REPLACE FUNCTION fnc_peer_task_xp() 
RETURNS TABLE (
    Peer           VARCHAR,
    TAsk           VARCHAR,
    XP             INT
) AS 
$fnc_peer_task_xp$
BEGIN
	RETURN QUERY
    SELECT Checks.Peer, Checks.Task, XP.XPAmount
    FROM Checks
    JOIN P2P ON P2P.Check_ = Checks.ID
    JOIN Verter ON Verter.Check_ = Checks.ID
    JOIN XP ON XP.Check_ = Checks.ID
    WHERE (Verter.State = 'Success' OR Verter.State IS NULL) AND P2P.State = 'Success';
END;
$fnc_peer_task_xp$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_peer_task_xp();

--3.3
CREATE OR REPLACE FUNCTION fnc_out_of_campus(pdate DATE) 
RETURNS TABLE (
    Peer           VARCHAR
) AS 
$fnc_out_of_campus$
BEGIN
	RETURN QUERY
	SELECT TimeTracking.peer
    FROM TimeTracking
	WHERE TimeTracking.Date = pdate AND state = 1
	EXCEPT
	SELECT TimeTracking.peer
    FROM TimeTracking
	WHERE TimeTracking.Date = pdate AND state = 2;
END;
$fnc_out_of_campus$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_out_of_campus('2023-11-17');

--3.4
CREATE OR REPLACE PROCEDURE prcdr_count_transferredpoints(IN RESULT refcursor)
AS 
$prcdr_count_transferredpoints$
BEGIN
	OPEN RESULT FOR
    WITH 	p1 AS (
        	SELECT Nickname, COALESCE(sum(PointsAmount), 0) as get
        	FROM TransferredPoints
        	RIGHT JOIN Peers ON TransferredPoints.CheckingPeer = Peers.Nickname
        	GROUP BY Nickname
    ),
    		p2 AS (
        	SELECT Nickname, COALESCE(sum(PointsAmount), 0) as give
        	FROM TransferredPoints
        	RIGHT JOIN Peers ON TransferredPoints.CheckedPeer = Peers.Nickname
        	GROUP BY Nickname
    )
    SELECT p1.Nickname, get - give AS PointsChange
    FROM p1
    JOIN p2 ON p2.Nickname = p1.Nickname;
END;
$prcdr_count_transferredpoints$ 
LANGUAGE plpgsql;

-- CALL prcdr_count_transferredpoints('result');
-- FETCH ALL IN "result";


--3.5
CREATE OR REPLACE PROCEDURE prcdr_count_transferredpoints_readble(IN RESULT refcursor) 
AS $prcdr_count_transferredpoints_readble$
BEGIN
	OPEN RESULT FOR
	WITH 	checking AS (SELECT peer1, SUM(PointsAmount) FROM  fnc_transfferedpoints_readble() GROUP BY peer1),
			checked  AS (SELECT peer2, SUM(PointsAmount) FROM  fnc_transfferedpoints_readble() GROUP BY peer2)
	SELECT COALESCE(checked.peer2, checking.peer1) AS peer, (COALESCE(checking.sum, 0) - COALESCE(checked.sum, 0)) AS PointsChange
	FROM checking
	FULL JOIN checked ON checking.peer1 = checked.peer2;
END;
$prcdr_count_transferredpoints_readble$ LANGUAGE plpgsql;

-- CALL prcdr_count_transferredpoints_readble('result');
-- FETCH ALL IN "result";

--3.6

CREATE OR REPLACE PROCEDURE prcdr_frequently_pd(IN RESULT refcursor) 
AS $prcdr_frequently_pd$ 
BEGIN
	OPEN RESULT FOR
	WITH p AS (
		SELECT Date, Task, (SELECT COUNT(Task) FROM Checks C WHERE C.Task = Ch.Task AND C.Date = Ch.Date) AS Amount
		FROM Checks Ch
	)
	SELECT DISTINCT Date, Task
	FROM p p1
	WHERE Amount = (SELECT MAX(Amount) FROM p p2 WHERE p1.Date = p2.Date)
	ORDER BY DATE DESC;
END;
$prcdr_frequently_pd$ LANGUAGE plpgsql;

-- CALL prcdr_frequently_pd('result');
-- FETCH ALL IN "result";

--3.7

CREATE OR REPLACE PROCEDURE prcdr_completed_brances(IN RESULT refcursor, block VARCHAR)
AS $prcdr_completed_brances$
BEGIN
	OPEN RESULT FOR 
	WITH tasks_block AS (
		SELECT Title
		FROM Tasks
		WHERE substring(title FROM '.+?(?=\d{1,2})') = block
	)
	SELECT DISTINCT Checks.Peer, MAX(Checks.Date) AS day
	FROM Checks
	JOIN XP ON XP.Check_ = Checks.ID
	WHERE Checks.Task IN (SELECT Title FROM tasks_block)
	GROUP BY Checks.Peer
	HAVING COUNT(DISTINCT Checks.Task) = (SELECT COUNT(*) FROM tasks_block);
END
$prcdr_completed_brances$ LANGUAGE plpgsql;

--CALL prcdr_completed_brances('result', 'CPP');
--FETCH ALL IN "result";

--3.8

CREATE OR REPLACE PROCEDURE prcdr_best_recommendations(IN RESULT refcursor)
AS $prcdr_best_recommendations$
BEGIN
	OPEN RESULT FOR
	WITH friendlist AS (
		SELECT peer1, peer2 FROM friends
		UNION
		SELECT peer2 as peer1, peer1 AS peer2 FROM friends),
	ranks AS (
		SELECT friendlist.peer1 AS peer, recommendations.recommendationspeer AS recommendedpeer, COUNT(recommendations.recommendationspeer)
		FROM friendlist
		JOIN recommendations ON recommendations.peer = friendlist.peer2
		WHERE friendlist.peer1 != recommendations.recommendationspeer
		GROUP BY friendlist.peer1, recommendations.recommendationspeer
		ORDER BY friendlist.peer1, COUNT(recommendations.recommendationspeer) DESC)
	SELECT peer, recommendedpeer
	FROM (SELECT peer, recommendedpeer, ROW_NUMBER() OVER (PARTITION BY peer ORDER BY count DESC) AS rank
		 FROM ranks) res
	WHERE rank = 1;
END;
$prcdr_best_recommendations$ LANGUAGE plpgsql;

-- CALL prcdr_best_recommendations('result');
-- FETCH ALL IN "result";

--3.9

CREATE OR REPLACE PROCEDURE prcdr_started_branches(IN RESULT refcursor, first_block VARCHAR, second_block VARCHAR)
AS $prcdr_started_branches$
DECLARE
    peer_count bigint;
BEGIN
	peer_count = (SELECT COUNT(*) FROM peers);
	OPEN RESULT FOR 
	WITH first_peers AS (
		SELECT DISTINCT peer
		FROM checks
		WHERE substring(task FROM '.+?(?=\d{1,2})') = first_block),
		second_peers AS (
			SELECT DISTINCT peer
			FROM checks
			WHERE substring(task FROM '.+?(?=\d{1,2})') = second_block),
		only_first AS (
			SELECT COUNT(Nickname)
			FROM peers
			WHERE Nickname IN (SELECT peer FROM first_peers) AND Nickname NOT IN (SELECT peer FROM second_peers)),
		only_second AS (
			SELECT COUNT(Nickname)
			FROM peers
			WHERE Nickname IN (SELECT peer FROM second_peers) AND Nickname NOT IN (SELECT peer FROM first_peers)),
		both_blocks AS (
			SELECT COUNT(Nickname)
			FROM peers
			WHERE Nickname IN (SELECT peer FROM second_peers) AND Nickname IN (SELECT peer FROM first_peers)),
		none_blocks AS (
			SELECT COUNT(Nickname)
			FROM peers
			WHERE Nickname NOT IN (SELECT peer FROM second_peers) AND Nickname NOT IN (SELECT peer FROM first_peers))
		SELECT only_first.count * 100 / peer_count AS StartedBlock1, only_second.count * 100 / peer_count AS StartedBlock2, 
			both_blocks.count * 100 / peer_count AS StartedBothBlocks, none_blocks.count * 100 / peer_count AS DidntStartAnyBlock
		FROM only_first
		CROSS JOIN only_second
		CROSS JOIN both_blocks
		CROSS JOIN none_blocks;
END;
$prcdr_started_branches$ LANGUAGE plpgsql;

-- CALL prcdr_started_branches('result', 'C', 'DO');
-- FETCH ALL IN "result";

-- 3.10

CREATE OR REPLACE PROCEDURE prcdr_birthday_projects(IN RESULT refcursor)
AS $prcdr_birthday_projects$
DECLARE
    peer_count bigint;
BEGIN
	peer_count = (SELECT COUNT(*) FROM peers);
	OPEN RESULT FOR 
	WITH birthday_successes AS (
		SELECT COUNT(DISTINCT peer)
		FROM checks
		JOIN p2p ON p2p.check_ = checks.id
		JOIN peers ON peers.Nickname = checks.peer
		JOIN verter ON verter.check_ = checks.id
		WHERE date_part('day', checks.date) = date_part('day', peers.birthday) 
		AND date_part('month', checks.date) = date_part('month', peers.birthday) AND verter.state = 'Success' AND p2p.state = 'Success'
		), birthday_fails AS (
		SELECT COUNT(DISTINCT peer)
		FROM checks
		JOIN p2p ON p2p.check_ = checks.id
		JOIN peers ON peers.Nickname = checks.peer
		JOIN verter ON verter.check_ = checks.id
		WHERE date_part('day', checks.date) = date_part('day', peers.birthday) 
		AND date_part('month', checks.date) = date_part('month', peers.birthday) AND (verter.state = 'Failure' OR p2p.state = 'Failure')
		)
	SELECT birthday_successes.count * 100 / peer_count AS SuccessfulChecks, birthday_fails.count * 100 / peer_count AS UnsuccessfulChecks
	FROM birthday_successes
	CROSS JOIN birthday_fails;
END;
$prcdr_birthday_projects$ LANGUAGE plpgsql;

-- CALL prcdr_birthday_projects('result');
-- FETCH ALL IN "result";

-- 3.11

CREATE OR REPLACE PROCEDURE prcdr_three_tasks(IN RESULT refcursor, first_task VARCHAR, second_task VARCHAR, third_task VARCHAR)
AS $prcdr_three_tasks$
BEGIN
	OPEN RESULT FOR
	WITH first_batch AS (
		SELECT DISTINCT peer
		FROM checks
		JOIN verter ON verter.Check_ = checks.id
		JOIN p2p ON p2p.check_ = checks.id
		WHERE task = first_task AND p2p.state = 'Success' AND verter.state = 'Success'),
		second_batch AS (
		SELECT DISTINCT peer
		FROM checks
		JOIN verter ON verter.Check_ = checks.id
		JOIN p2p ON p2p.check_ = checks.id
		WHERE task = second_task AND p2p.state = 'Success' AND verter.state = 'Success'),
		third_batch AS (
		SELECT DISTINCT peer
		FROM checks
		JOIN verter ON verter.Check_ = checks.id
		JOIN p2p ON p2p.check_ = checks.id
		WHERE task = third_task AND p2p.state = 'Success' AND verter.state = 'Success')
	SELECT Nickname
	FROM peers
	WHERE Nickname IN (SELECT peer FROM first_batch) AND Nickname IN (SELECT peer FROM second_batch) AND 
		Nickname NOT IN (SELECT peer FROM third_batch);
END;
$prcdr_three_tasks$ LANGUAGE plpgsql;

-- CALL prcdr_three_tasks('result', 'C2_SimpleBashUtils', 'C3_s21_string+', 'DO1_Linux');
-- FETCH ALL IN "result";

-- 3.12

CREATE OR REPLACE FUNCTION get_prev_count()
RETURNS TABLE ("Task" VARCHAR, "PrevCount" INT) AS $$
BEGIN
	RETURN QUERY
	WITH RECURSIVE Stat AS (
	   SELECT Title AS Task, 0 AS PrevCount
	     FROM Tasks
	    WHERE ParentTask IS NULL
	UNION ALL 
	   SELECT Title, PrevCount + 1
		 FROM Tasks
			  JOIN Stat ON ParentTask = Task
	)
	SELECT Task, PrevCount
	  FROM Stat;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_prev_count();

-- 3.13

CREATE OR REPLACE FUNCTION get_lucky_days(n INT)
RETURNS TABLE ("Day" DATE) AS $$
BEGIN
	RETURN QUERY
	WITH success_checks AS (
      SELECT checks.id, task, date, time, state, XPAmount
        FROM checks
	  	     JOIN p2p   ON checks.id = p2p.id
             JOIN xp    ON checks.id = xp.id
	   WHERE (state = 'Success' OR state = 'Failure')
    ORDER BY date, time
    ), success_dates AS (
        SELECT id, date, time, state,
               (CASE WHEN (state = 'Success' AND XPAmount >= MaxXP * 0.8) THEN row_number() over (partition by state, date) ELSE 0 END) AS amount
          FROM success_checks
		       JOIN tasks ON title = task
      ORDER BY date
    ), max_dates AS (
        SELECT date, MAX(amount) AS amount 
          FROM success_dates
      GROUP BY date
    )
    SELECT date AS day 
      FROM max_dates 
     WHERE amount >= n;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_lucky_days(2);

-- 3.14

CREATE OR REPLACE FUNCTION get_peer_with_max_xp()
RETURNS TABLE ("Peer" VARCHAR, "XP" BIGINT) AS $$
BEGIN
	RETURN QUERY
      SELECT Peer, SUM(XPAmount) AS XP
        FROM XP
		     JOIN Checks ON Check_ = Checks.ID
	GROUP BY Peer
	ORDER BY XP DESC
	   LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_peer_with_max_xp();

-- 3.15

CREATE OR REPLACE FUNCTION get_peers_early_comming(t TIME, n INT)
RETURNS TABLE ("Peer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
      SELECT peer
        FROM (  SELECT peer
				  FROM timetracking
			  GROUP BY peer, date
		        HAVING MIN(time) < t ) AS query_in
	GROUP BY peer
      HAVING COUNT(peer) >= n;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_peers_early_comming(TIME '17:00:00', 55);

-- 3.16

CREATE OR REPLACE FUNCTION get_peers_entries(n INT, m INT)
RETURNS TABLE ("Peer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	WITH entries_in_period AS (
	SELECT peer, date, count
      FROM (  SELECT peer, date, COUNT(state) - 1 AS count
		  	    FROM timetracking
	   	  	   WHERE state = 2
		  	GROUP BY peer, date) AS query_in
	 WHERE (CURRENT_DATE - date) < n
    )
	  SELECT peer FROM entries_in_period
	GROUP BY peer, count
	  HAVING count >= m;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_peers_entries(5, 0);

-- 3.17

CREATE OR REPLACE FUNCTION early_entries()
RETURNS TABLE("Month" TEXT, "EarlyEntries" NUMERIC) AS $$
BEGIN
	  RETURN QUERY
	  SELECT Month, ROUND((COUNT(Entries) FILTER (WHERE Entries < TIME '12:00:00'))::DECIMAL / COUNT(Entries) * 100) AS EarlyEntries
	    FROM (  SELECT TO_CHAR(Birthday, 'Month') AS Month, MIN(time) AS Entries
	    	      FROM timetracking
		          JOIN peers ON peer = nickname
	          GROUP BY TO_CHAR(Birthday, 'Month'), peer, date ) AS query_in
	GROUP BY Month
	ORDER BY TO_DATE(Month, 'Month');
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM early_entries();
