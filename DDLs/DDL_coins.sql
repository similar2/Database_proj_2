create table if not exists coins
(
    BV_coin  char(12) REFERENCES videorecord (BV),
    mid_coin bigint references userrecord (mid)
);