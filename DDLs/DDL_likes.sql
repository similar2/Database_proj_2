create table if not exists likes
(
    BV_liked  char(12) REFERENCES videorecord (BV),
    mid_liked bigint references userrecord (mid)
);

