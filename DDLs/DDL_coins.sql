create table if not exists coins
(
    BV_coin  varchar(12) REFERENCES videorecord (BV),
    mid_coin bigint references userrecord (mid)
);