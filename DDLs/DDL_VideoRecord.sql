CREATE TABLE if not exists VideoRecord
(
    bv          VARCHAR(255) PRIMARY KEY,           -- unique identification string of a video
    title       VARCHAR(255),                       -- name of the video
    ownerMid    bigint REFERENCES UserRecord (mid), -- mid of the video owner
    ownerName   VARCHAR(255),                       -- name of the video owner
    commitTime  TIMESTAMP,                          -- time of video commitment
    reviewTime  TIMESTAMP,                          -- time of video review
    publicTime  TIMESTAMP,                          -- time of video publication
    duration    INT,                                -- duration of the video in seconds
    description TEXT,                               -- brief introduction of the video
    reviewer    bigint REFERENCES UserRecord (mid)  -- mid of the video reviewer
);