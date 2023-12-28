create table if not exists favorites
(
    BV_favorite  varchar(12) REFERENCES videorecord (BV),
    mid_favorite bigint references userrecord (mid)
);
