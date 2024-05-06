DROP TABLE IF EXISTS Peers CASCADE;
DROP TABLE IF EXISTS Verter CASCADE;
DROP TABLE IF EXISTS Tasks CASCADE;
DROP TABLE IF EXISTS Friends CASCADE;
DROP TABLE IF EXISTS Checks CASCADE;
DROP TABLE IF EXISTS TransferredPoints CASCADE;
DROP TABLE IF EXISTS P2P CASCADE;
DROP TABLE IF EXISTS XP CASCADE;
DROP TABLE IF EXISTS TimeTracking CASCADE;
DROP TABLE IF EXISTS Recommendations CASCADE;
DROP TYPE IF EXISTS CHECK_STATUS CASCADE;

CREATE TABLE IF NOT EXISTS Peers (
    Nickname            VARCHAR                                 NOT NULL PRIMARY KEY,
    Birthday            DATE                                    NOT NULL
);

CREATE TABLE Tasks (
    Title               VARCHAR                                 NOT NULL PRIMARY KEY,
    ParentTask          VARCHAR                                 ,
    MaxXP               INT                                     NOT NULL,

    CONSTRAINT          fk_Tasks_ParentTask                     FOREIGN KEY (ParentTask)                REFERENCES Tasks(Title),
    CONSTRAINT          c_Tasks_MaxXP                           CHECK (MaxXP >= 0)  
);

CREATE TYPE CHECK_STATUS AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS Checks (
    ID                  BIGINT                                  NOT NULL PRIMARY KEY,
    Peer                VARCHAR                                 NOT NULL,
    Task                VARCHAR                                 NOT NULL,
    Date                DATE                                    NOT NULL DEFAULT CURRENT_DATE,

    CONSTRAINT          fk_Checks_Peer                          FOREIGN KEY (Peer)                      REFERENCES Peers(Nickname),
    CONSTRAINT          fk_Checks_Task                          FOREIGN KEY (Task)                      REFERENCES Tasks(Title) 
);

CREATE TABLE IF NOT EXISTS P2P (
    ID                  BIGINT                                  NOT NULL PRIMARY KEY,
    Check_              BIGINT                                  NOT NULL,
    CheckingPeer        VARCHAR                                 NOT NULL,
    State               CHECK_STATUS                            NOT NULL,
    Time                TIME NOT NULL                           DEFAULT CURRENT_TIME,

    CONSTRAINT          fk_P2P_Check                            FOREIGN KEY (Check_)                    REFERENCES Checks(ID),
    CONSTRAINT          fk_P2P_CheckingPeer                     FOREIGN KEY (CheckingPeer)              REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Verter (
    ID                  BIGINT NOT NULL PRIMARY KEY,
    Check_              BIGINT NOT NULL,
    State               CHECK_STATUS NOT NULL,
    Time                TIME NOT NULL DEFAULT CURRENT_TIME,

    CONSTRAINT          fk_Verter_Check                         FOREIGN KEY (Check_)                    REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS TransferredPoints (
    ID                  BIGINT NOT NULL PRIMARY KEY,
    CheckingPeer        VARCHAR NOT NULL,
    CheckedPeer         VARCHAR NOT NULL,
    PointsAmount        INT NOT NULL,

    CONSTRAINT          fk_TransferredPoints_CheckingPeer       FOREIGN KEY (CheckingPeer)              REFERENCES Peers(Nickname),
    CONSTRAINT          fk_TransferredPoints_CheckedPeer        FOREIGN KEY (CheckedPeer)               REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Friends (
    ID                  BIGINT NOT NULL PRIMARY KEY,
    Peer1               VARCHAR NOT NULL,
    Peer2               VARCHAR NOT NULL,

    CONSTRAINT          fk_Friends_Peer1                        FOREIGN KEY (Peer1)                     REFERENCES Peers(Nickname),
    CONSTRAINT          fk_Friends_Peer2                        FOREIGN KEY (Peer2)                     REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Recommendations (
    ID                  BIGINT                                  NOT NULL PRIMARY KEY,
    Peer                VARCHAR                                 NOT NULL,
    RecommendationsPeer VARCHAR                                 NOT NULL,

    CONSTRAINT          fk_Recommendations_Peer                 FOREIGN KEY (Peer)                      REFERENCES Peers(Nickname),
    CONSTRAINT          fk_Recommendations_RecommendationsPeer  FOREIGN KEY (RecommendationsPeer)       REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS XP (
    ID                  BIGINT                                  NOT NULL PRIMARY KEY,
    Check_              BIGINT                                  NOT NULL,
    XPAmount            INT                                     NOT NULL,

    CONSTRAINT          fk_XP_Check                             FOREIGN KEY (Check_)                    REFERENCES Checks(ID),
    CONSTRAINT          c_XP_XPAmount                           CHECK (XPAmount >= 0)
);

CREATE TABLE IF NOT EXISTS TimeTracking (
    ID                  BIGINT                                  NOT NULL PRIMARY KEY,
    Peer                VARCHAR                                 NOT NULL,
    Date                DATE                                    NOT NULL DEFAULT CURRENT_DATE,
    Time                TIME                                    NOT NULL DEFAULT CURRENT_TIME,
    State               SMALLINT                                NOT NULL,

    CONSTRAINT          fk_TimeTracking_Peer                    FOREIGN KEY (Peer)                      REFERENCES Peers(Nickname),
    CONSTRAINT          c_TimeTracking_State                    CHECK (State BETWEEN 1 AND 2)
);

CREATE OR REPLACE PROCEDURE prcdr_import_csv (
    IN table_name       TEXT,
    IN file_name        TEXT,
    IN delimiter        CHAR DEFAULT ','
) AS $prcdr_import_csv$
BEGIN EXECUTE format(
    'COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;',
    table_name,
    file_name,
    delimiter
);
END;
$prcdr_import_csv$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE prcdr_export_csv (
    IN table_name       TEXT,
    IN file_name        TEXT,
    IN delimiter        CHAR DEFAULT ','
) AS $prcdr_export_csv$
BEGIN EXECUTE format(
    'COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;',
    table_name,
    file_name,
    delimiter
);
END;
$prcdr_export_csv$ LANGUAGE plpgsql;

CALL prcdr_import_csv('Peers', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/peers.csv', ',');
CALL prcdr_import_csv('Tasks', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/tasks.csv', ',');
CALL prcdr_import_csv('Checks', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/checks.csv', ',');
CALL prcdr_import_csv('P2P', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/p2p.csv', ',');
CALL prcdr_import_csv('Verter', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/verter.csv', ',');
CALL prcdr_import_csv('TransferredPoints', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/transferredpoints.csv', ',');
CALL prcdr_import_csv('Friends', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/friends.csv', ',');
CALL prcdr_import_csv('Recommendations', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/recommendations.csv', ',');
CALL prcdr_import_csv('XP', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/xp.csv', ',');
CALL prcdr_import_csv('TimeTracking', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csv/timetracking.csv', ',');

-- CALL prcdr_export_csv('Peers', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/peers.csv', ',');
-- CALL prcdr_export_csv('Tasks', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/tasks.csv', ',');
-- CALL prcdr_export_csv('Checks', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/checks.csv', ',');
-- CALL prcdr_export_csv('P2P', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/p2p.csv', ',');
-- CALL prcdr_export_csv('Verter', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/verter.csv', ',');
-- CALL prcdr_export_csv('TransferredPoints', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/transferredpoints.csv', ',');
-- CALL prcdr_export_csv('Friends', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/friends.csv', ',');
-- CALL prcdr_export_csv('Recommendations', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/recommendations.csv', ',');
-- CALL prcdr_export_csv('XP', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/xp.csv', ',');
-- CALL prcdr_export_csv('TimeTracking', '/Users/argoniaz/Desktop/SQL2_Info21_v1.0-1/src/csvE/timetracking.csv', ',');
