
CREATE DATABASE IF NOT EXISTS olympic_dataset;
USE olympic_dataset;


CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition      VARCHAR(100),
    edition_id   VARCHAR(20),
    country_noc  VARCHAR(10),
    sport        VARCHAR(100),
    event        VARCHAR(500),
    result_id    VARCHAR(20),
    athlete      VARCHAR(255),
    athlete_id   VARCHAR(20),
    pos          VARCHAR(50),
    medal        VARCHAR(20),
    isTeamSport  VARCHAR(10)
);


LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE athlete_event_results
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


GRANT ALL PRIVILEGES ON olympic_dataset.* TO 'airflow_user'@'%';
FLUSH PRIVILEGES;