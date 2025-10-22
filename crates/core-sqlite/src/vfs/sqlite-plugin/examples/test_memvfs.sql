-- Load the memvfs extension and open a new connection using it
-- Build the memvfs extension using the following command:
--   cargo build --example memvfs --features dynamic

-- uncomment to enable verbose logs
-- .log stderr

.load target/debug/examples/libmemvfs.so
.open main.db

.databases
.vfsinfo

CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;
pragma hello_vfs=1234;

select * from dbstat;

vacuum;
drop table t1;
vacuum;

select * from dbstat;
