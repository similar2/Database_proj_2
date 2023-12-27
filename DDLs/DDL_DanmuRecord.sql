CREATE TABLE if not exists DanmuRecord
(
    danmu_id serial primary key,
    bv       VARCHAR(255) REFERENCES VideoRecord (bv), -- video's BV for the danmu
    mid      bigint REFERENCES UserRecord (mid),          -- sender's mid of the danmu
    time     float,                                    -- display time of the danmu in seconds since video starts
    content  TEXT,                                     -- content of the danmu
    postTime TIMESTAMP,                                -- post time of the danmu
    likedBy  BIGINT[]                                  -- mids of users who liked the danmu
);
