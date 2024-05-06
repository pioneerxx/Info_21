CREATE OR REPLACE PROCEDURE add_p2p_check(
    checkedPeer_     VARCHAR, 
    checkingPeer_    VARCHAR, 
    taskTitle_       VARCHAR, 
    checkStatus_     CHECK_STATUS, 
    time_           TIME
    )
AS $add_p2p_check$
DECLARE
    lastStatus CHECK_STATUS;
BEGIN
lastStatus := (
        SELECT State
        FROM P2P
        JOIN Checks ON Checks.ID = P2P.Check_ 
        WHERE P2P.CheckingPeer = checkingPeer_ AND Checks.Peer = checkedPeer_ AND Checks.Task = taskTitle_
        ORDER BY P2P.ID DESC
        LIMIT 1
    );
	RAISE NOTICE 'Value of status_value is %', lastStatus; -- вывод значения переменной
IF checkStatus_ = 'Start' AND (lastStatus != 'Start' OR lastStatus IS NULL) THEN
	INSERT INTO Checks
	VALUES ((SELECT MAX(ID) FROM Checks) + 1, checkedPeer_, taskTitle_);
	INSERT INTO P2P
	VALUES ((SELECT MAX(ID) FROM P2P) + 1,(SELECT MAX(ID) FROM Checks), checkingPeer_, checkStatus_, time_);
ELSEIF checkStatus_ != 'Start' AND lastStatus = 'Start' THEN
	INSERT INTO P2P
	VALUES ((SELECT MAX(ID) FROM P2P) + 1, (SELECT Check_
	FROM P2P
	JOIN Checks ON Checks.ID = P2P.Check_ 
	WHERE P2P.CheckingPeer = checkingPeer_ AND Checks.Peer = checkedPeer_ AND Checks.Task = taskTitle_
	ORDER BY P2P.ID DESC
	LIMIT 1), checkingPeer_, checkStatus_, time_);
END IF;
END
$add_p2p_check$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE add_verter_check(
    checkedPeer_     VARCHAR, 
    taskTitle_       VARCHAR,  
    checkStatus_     CHECK_STATUS, 
    time_           TIME
    )
AS $add_verter_check$
DECLARE 
    checkID_ INT := (
        SELECT Checks.ID
        FROM Checks
        JOIN P2P ON P2P.Check_ = Checks.ID AND P2P.State = 'Success'
        WHERE Checks.Task = taskTitle_ AND Checks.Peer = checkedPeer_
        ORDER BY Checks.Task DESC, P2P.Time DESC
        LIMIT 1
    );
BEGIN
    IF (checkID_ != 0) THEN
    INSERT INTO Verter
    VALUES ((SELECT MAX(ID) FROM Verter) + 1, checkID_, checkStatus_, time_);
    END IF;
END;
$add_verter_check$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trg_fnc_add_p2p_transfferedpoints () 
RETURNS TRIGGER AS $trg_fnc_add_p2p_transfferedpoints$
DECLARE
    checkedPeer VARCHAR;
BEGIN
    checkedPeer := (
        SELECT Peer 
        FROM Checks 
        WHERE ID = NEW.Check_ 
        LIMIT 1
    );
    IF (new.State = 'Start') THEN
        IF EXISTS (
            SELECT *
            FROM TransferredPoints
            WHERE CheckedPeer = checkedPeer AND CheckingPeer = NEW.CheckingPeer 
        ) THEN
            UPDATE TransferredPoints
            SET PointsAmount = PointsAmount + 1
            WHERE CheckedPeer = checkedPeer AND CheckingPeer = NEW.CheckingPeer; 
        ELSE 
            INSERT INTO TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount)
            VALUES (NEW.CheckingPeer, checkedPeer, 1);
        END IF;
    END IF;
    RETURN NEW;
END;
$trg_fnc_add_p2p_transfferedpoints$  LANGUAGE plpgsql;

CREATE TRIGGER trg_add_p2p_transfferedpoints
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE PROCEDURE trg_fnc_add_p2p_transfferedpoints();

CREATE OR REPLACE FUNCTION trg_fnc_add_xp () 
RETURNS TRIGGER AS $trg_fnc_add_xp$
DECLARE
    max_xp INT := (
        SELECT MaxXP
        FROM Tasks
        WHERE Title = (SELECT Task FROM Checks WHERE Checks.ID = NEW.Check_)
    );
    p2p_state CHECK_STATUS := (
        SELECT State
        FROM P2P
        WHERE P2P.Check_ = NEW.Check_
        ORDER BY P2P.ID DESC 
        LIMIT 1
    );
    verter_state CHECK_STATUS := (
        SELECT State
        FROM Verter
        WHERE Verter.Check_ = NEW.Check_
        ORDER BY Verter.ID DESC 
        LIMIT 1
    );
BEGIN
    IF NOT (p2p_state = 'Success' AND (verter_state IS NULL OR verter_state = 'Success')) THEN
        RAISE EXCEPTION 'check fail';
    ELSIF NEW.XPAmount > max_xp THEN
        RAISE EXCEPTION 'XP > MaxXP';
    END IF;
    RETURN NEW;
END;
$trg_fnc_add_xp$  LANGUAGE plpgsql;

CREATE TRIGGER trg_add_xp
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE PROCEDURE trg_fnc_add_xp();
