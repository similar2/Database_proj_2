<<<<<<< HEAD
create table if not exists favorites
(
    BV_favorite  char(12) REFERENCES videorecord (BV),
    mid_favorite bigint references userrecord (mid)
);
=======
create table if not exists favorites
(
    BV_favorite  char(12) REFERENCES videorecord (BV),
    mid_favorite bigint references userrecord (mid)
);
>>>>>>> 58a23ada817ce4ec67233d4a96d5163af185d2d6
