CREATE TABLE if not exists PostVideoReq (
    title VARCHAR(255), -- title of the video
    description TEXT, -- description of the video
    duration INT, -- duration of the video in seconds
    publicTime TIMESTAMP -- scheduled public time of the video
);
