CREATE KEYSPACE testkeyspace WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE TYPE testkeyspace.cidr_range (
    id uuid,
    start inet,
    end inet,
    description text
);

CREATE TABLE testkeyspace.testtable (
                                        partition_key_1 uuid,
                                        partition_key_2 text,
                                        partition_key_3 frozen<set<bigint>>,
                                        clustering_column_1 date,
                                        clustering_column_2 inet,
                                        column_1 blob,
                                        column_2 duration,
                                        column_3 map<text, bigint>,
                                        PRIMARY KEY ((partition_key_1, partition_key_2, partition_key_3), clustering_column_1, clustering_column_2)
    ) WITH CLUSTERING ORDER BY (clustering_column_1 ASC, clustering_column_2 ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE testkeyspace.testtable2 (
                                         partition_key_1 uuid,
                                         partition_key_2 text,
                                         column_1 testkeyspace.cidr_range,
                                         PRIMARY KEY ((partition_key_1, partition_key_2))
    ) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;
