CREATE TABLE if not exists ViewRecord
(

    bv        varchar(255) references videorecord (bv),
    mid       INT REFERENCES UserRecord (mid), -- mid of the user who watched the video
    timestamp TIMESTAMP                        -- last watch timestamp
);
