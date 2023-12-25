drop schema public cascade;
create schema "public";

CREATE TABLE if not exists UserRecord
(
    mid        bigint PRIMARY KEY,                                           -- unique identification number for the user
    name       VARCHAR(255),                                                 -- name created by the user
    sex        VARCHAR(50),                                                  -- biological sex, or other gender identities
    birthday   DATE,                                                         -- birthday of the user
    level      INT,                                                          -- user engagement level evaluated by system criteria
    sign       TEXT,                                                         -- personal description created by the user
    following  bigint[],                                                     -- list of mids of followed users
    identity   VARCHAR(50) CHECK (lower(identity) IN ('user', 'superuser')), -- role of the user
    password   VARCHAR(255) not null,                                        -- password for login
    qq         VARCHAR(255),                                                 -- OIDC login with QQ, no password required
    wechat     VARCHAR(255),                                                 -- OIDC login with WeChat, no password required
    is_deleted bool default false,
    coin       int                                                           -- initial number of coins
);

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

CREATE TABLE if not exists AuthInfo
(
    mid      bigint PRIMARY KEY REFERENCES UserRecord (mid), -- user's mid, foreign key to UserRecord
    password VARCHAR(255),                                   -- password for login
    qq       VARCHAR(255),                                   -- OIDC login with QQ, no password required
    wechat   VARCHAR(255)                                    -- OIDC login with WeChat, no password required
);

CREATE TABLE if not exists ViewRecord
(

    bv        varchar(255) references videorecord (bv),
    mid       bigint REFERENCES UserRecord (mid), -- mid of the user who watched the video
    timestamp float                               -- last watch timestamp
);


CREATE TABLE if not exists DanmuRecord
(
    danmu_id serial primary key,
    bv       VARCHAR(255) REFERENCES VideoRecord (bv), -- video's BV for the danmu
    mid      bigint REFERENCES UserRecord (mid),       -- sender's mid of the danmu
    time     float,                                    -- display time of the danmu in seconds since video starts
    content  TEXT,                                     -- content of the danmu
    postTime TIMESTAMP,                                -- post time of the danmu
    likedBy  BIGINT[]                                  -- mids of users who liked the danmu
);


CREATE TABLE if not exists PostVideoReq
(
    title       VARCHAR(255), -- title of the video
    description TEXT,         -- description of the video
    duration    INT,          -- duration of the video in seconds
    publicTime  TIMESTAMP     -- scheduled public time of the video
);


CREATE TABLE if not exists UserInfoResp
(
    mid       bigint PRIMARY KEY REFERENCES UserRecord (mid), -- user's mid, foreign key to UserRecord
    coin      INT,                                            -- number of user's coins
    following bigint[],                                       -- list of mids of followed users
    follower  bigint[],                                       -- list of follower mids
    watched   VARCHAR(255)[],                                 -- BVs of watched videos
    liked     VARCHAR(255)[],                                 -- BVs of liked videos
    collected VARCHAR(255)[],                                 -- BVs of collected videos
    posted    VARCHAR(255)[]                                  -- BVs of posted videos
);

create table if not exists likes
(
    BV_liked  char(12) REFERENCES videorecord (BV),
    mid_liked bigint references userrecord (mid)
);


create table if not exists favorites
(
    BV_favorite  char(12) REFERENCES videorecord (BV),
    mid_favorite bigint references userrecord (mid)
);
create table if not exists coins
(
    BV_coin  char(12) REFERENCES videorecord (BV),
    mid_coin bigint references userrecord (mid)
);

