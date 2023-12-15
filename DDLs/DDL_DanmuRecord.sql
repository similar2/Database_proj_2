CREATE TABLE if not exists DanmuRecord
(
    danmu_id serial primary key,
    bv       VARCHAR(255) REFERENCES VideoRecord (bv), -- video's BV for the danmu
    mid      INT REFERENCES UserRecord (mid),          -- sender's mid of the danmu
    time     float,                                    -- display time of the danmu in seconds since video starts
    content  TEXT,                                     -- content of the danmu
    postTime TIMESTAMP,                                -- post time of the danmu
    likedBy  BIGINT[]                                  -- mids of users who liked the danmu
);
create table if not exists restricted_words
(
    id   serial primary key,
    word varchar(255) not null
);


create or replace trigger dirty_words
    before insert
    on danmurecord
    for each row
execute procedure content_check();

CREATE OR REPLACE FUNCTION content_check()
    RETURNS TRIGGER AS
$$
DECLARE
    word record;
begin
    for word in select * from restricted_words
        loop
            if lower(word) = lower(new.content) then
                return null;
            end if;
        end loop;
end ;
$$ language plpgsql;
