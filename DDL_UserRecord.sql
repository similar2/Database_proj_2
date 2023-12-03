CREATE TABLE if not exists UserRecord (
    mid SERIAL PRIMARY KEY, -- unique identification number for the user
    name VARCHAR(255), -- name created by the user
    sex VARCHAR(50), -- biological sex, or other gender identities
    birthday DATE, -- birthday of the user
    level INT, -- user engagement level evaluated by system criteria
    sign TEXT, -- personal description created by the user
    following INT[], -- list of mids of followed users
    identity VARCHAR(50) CHECK (identity IN ('user', 'superuser')), -- role of the user
    password VARCHAR(255), -- password for login
    qq VARCHAR(255), -- OIDC login with QQ, no password required
    wechat VARCHAR(255) -- OIDC login with WeChat, no password required
);
