CREATE TABLE if not exists ViewRecord
(

    bv        varchar(255) references videorecord (bv),
    mid       bigint REFERENCES UserRecord (mid), -- mid of the user who watched the video
    timestamp float                        -- last watch timestamp
);
