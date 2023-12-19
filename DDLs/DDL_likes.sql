<<<<<<< HEAD
create table if not exists likes
(
    BV_liked  char(12) REFERENCES videorecord (BV),
    mid_liked bigint references userrecord (mid)
);

=======
create table if not exists likes
(
    BV_liked  char(12) REFERENCES videorecord (BV),
    mid_liked bigint references userrecord (mid)
);

>>>>>>> 58a23ada817ce4ec67233d4a96d5163af185d2d6
