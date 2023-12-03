CREATE TABLE if not exists AuthInfo (
    mid INT PRIMARY KEY REFERENCES UserRecord(mid), -- user's mid, foreign key to UserRecord
    password VARCHAR(255), -- password for login
    qq VARCHAR(255), -- OIDC login with QQ, no password required
    wechat VARCHAR(255) -- OIDC login with WeChat, no password required
);
