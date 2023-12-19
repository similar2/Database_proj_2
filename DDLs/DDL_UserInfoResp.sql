CREATE TABLE if not exists UserInfoResp (
    mid bigint PRIMARY KEY REFERENCES UserRecord(mid), -- user's mid, foreign key to UserRecord
    coin INT, -- number of user's coins
    following INT[], -- list of mids of followed users
    follower INT[], -- list of follower mids
    watched VARCHAR(255)[], -- BVs of watched videos
    liked VARCHAR(255)[], -- BVs of liked videos
    collected VARCHAR(255)[], -- BVs of collected videos
    posted VARCHAR(255)[] -- BVs of posted videos
);
