DROP TABLE IF EXISTS covidm.monitor;
DROP SCHEMA IF EXISTS covidm;
CREATE SCHEMA covidm;
CREATE TABLE covidm.monitor (
    country VARCHAR(255),
    countrycode VARCHAR(50),
    slug VARCHAR(255),
    newconfirmed INT,
    totalconfirmed INT,
    newdeaths INT,
    totaldeaths INT,
    newrecovered INT,
    totalrecovered INT,
    updateddate TIMESTAMP
);