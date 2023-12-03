CREATE TABLE if not exists ViewRecord (
    mid INT REFERENCES UserRecord(mid), -- mid of the user who watched the video
    timestamp TIMESTAMP -- last watch timestamp
);
