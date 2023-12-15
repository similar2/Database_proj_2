CREATE TABLE if not exists DanmuRecord (
    bv VARCHAR(255) REFERENCES VideoRecord(bv), -- video's BV for the danmu
    mid INT REFERENCES UserRecord(mid), -- sender's mid of the danmu
    time INT, -- display time of the danmu in seconds since video starts
    content TEXT, -- content of the danmu
    postTime TIMESTAMP, -- post time of the danmu
    likedBy INT[] -- mids of users who liked the danmu
);
