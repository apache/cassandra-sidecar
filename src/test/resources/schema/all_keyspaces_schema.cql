CREATE KEYSPACE system_traces WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '2' } AND DURABLE_WRITES = true;

CREATE TABLE system_traces.events (
    session_id uuid,
    event_id timeuuid,
    activity text,
    source inet,
    source_elapsed int,
    source_port int,
    thread text,
    PRIMARY KEY (session_id, event_id)
) WITH CLUSTERING ORDER BY (event_id ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'tracing events'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_traces.sessions (
    session_id uuid,
    client inet,
    command text,
    coordinator inet,
    coordinator_port int,
    duration int,
    parameters map<text, text>,
    request text,
    started_at timestamp,
    PRIMARY KEY (session_id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'tracing sessions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE KEYSPACE system WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;

CREATE TABLE system.\"IndexInfo\" (
    table_name text,
    index_name text,
    value blob,
    PRIMARY KEY (table_name, index_name)
) WITH CLUSTERING ORDER BY (index_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'built column indexes'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.available_ranges (
    keyspace_name text,
    ranges set<blob>,
    PRIMARY KEY (keyspace_name)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'available keyspace/ranges during bootstrap/replace that are ready to be served'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.available_ranges_v2 (
    keyspace_name text,
    full_ranges set<blob>,
    transient_ranges set<blob>,
    PRIMARY KEY (keyspace_name)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'available keyspace/ranges during bootstrap/replace that are ready to be served'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.batches (
    id timeuuid,
    mutations list<blob>,
    version int,
    PRIMARY KEY (id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'batches awaiting replay'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 2 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.built_views (
    keyspace_name text,
    view_name text,
    status_replicated boolean,
    PRIMARY KEY (keyspace_name, view_name)
) WITH CLUSTERING ORDER BY (view_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'built views'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.compaction_history (
    id timeuuid,
    bytes_in bigint,
    bytes_out bigint,
    columnfamily_name text,
    compacted_at timestamp,
    keyspace_name text,
    rows_merged map<int, bigint>,
    PRIMARY KEY (id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'week-long compaction history'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 604800
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.local (
    key text,
    bootstrapped text,
    broadcast_address inet,
    broadcast_port int,
    cluster_name text,
    cql_version text,
    data_center text,
    gossip_generation int,
    host_id uuid,
    listen_address inet,
    listen_port int,
    native_protocol_version text,
    partitioner text,
    rack text,
    release_version text,
    rpc_address inet,
    rpc_port int,
    schema_version uuid,
    tokens set<text>,
    truncated_at map<uuid, blob>,
    PRIMARY KEY (key)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'information about the local node'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.paxos (
    row_key blob,
    cf_id uuid,
    in_progress_ballot timeuuid,
    in_progress_read_ballot timeuuid,
    most_recent_commit blob,
    most_recent_commit_at timeuuid,
    most_recent_commit_version int,
    proposal blob,
    proposal_ballot timeuuid,
    proposal_version int,
    PRIMARY KEY (row_key, cf_id)
) WITH CLUSTERING ORDER BY (cf_id ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'in-progress paxos proposals'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE CUSTOM INDEX \"PaxosUncommittedIndex\" ON system.paxos () USING 'org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedIndex' ;

CREATE TABLE system.paxos_repair_history (
    keyspace_name text,
    table_name text,
    points frozen<list<frozen<tuple<blob, timeuuid>>>>,
    PRIMARY KEY (keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (table_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'paxos repair history'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.peer_events (
    peer inet,
    hints_dropped map<timeuuid, int>,
    PRIMARY KEY (peer)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'events related to peers'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.peer_events_v2 (
    peer inet,
    peer_port int,
    hints_dropped map<timeuuid, int>,
    PRIMARY KEY (peer, peer_port)
) WITH CLUSTERING ORDER BY (peer_port ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'events related to peers'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.peers (
    peer inet,
    data_center text,
    host_id uuid,
    preferred_ip inet,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    tokens set<text>,
    PRIMARY KEY (peer)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'information about known peers in the cluster'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.peers_v2 (
    peer inet,
    peer_port int,
    data_center text,
    host_id uuid,
    native_address inet,
    native_port int,
    preferred_ip inet,
    preferred_port int,
    rack text,
    release_version text,
    schema_version uuid,
    tokens set<text>,
    PRIMARY KEY (peer, peer_port)
) WITH CLUSTERING ORDER BY (peer_port ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'information about known peers in the cluster'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.prepared_statements (
    prepared_id blob,
    logged_keyspace text,
    query_string text,
    PRIMARY KEY (prepared_id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'prepared statements'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.repairs (
    parent_id timeuuid,
    cfids set<uuid>,
    coordinator inet,
    coordinator_port int,
    last_update timestamp,
    participants set<inet>,
    participants_wp set<text>,
    ranges set<blob>,
    repaired_at timestamp,
    started_at timestamp,
    state int,
    PRIMARY KEY (parent_id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'repairs'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.size_estimates (
    keyspace_name text,
    table_name text,
    range_start text,
    range_end text,
    mean_partition_size bigint,
    partitions_count bigint,
    PRIMARY KEY (keyspace_name, table_name, range_start, range_end)
) WITH CLUSTERING ORDER BY (table_name ASC, range_start ASC, range_end ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'per-table primary range size estimates, table is deprecated in favor of table_estimates'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.sstable_activity (
    keyspace_name text,
    columnfamily_name text,
    generation int,
    rate_120m double,
    rate_15m double,
    PRIMARY KEY ((keyspace_name, columnfamily_name, generation))
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'historic sstable read rates'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.sstable_activity_v2 (
    keyspace_name text,
    table_name text,
    id text,
    rate_120m double,
    rate_15m double,
    PRIMARY KEY ((keyspace_name, table_name, id))
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'historic sstable read rates'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.table_estimates (
    keyspace_name text,
    table_name text,
    range_type text,
    range_start text,
    range_end text,
    mean_partition_size bigint,
    partitions_count bigint,
    PRIMARY KEY (keyspace_name, table_name, range_type, range_start, range_end)
) WITH CLUSTERING ORDER BY (table_name ASC, range_type ASC, range_start ASC, range_end ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'per-table range size estimates'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.top_partitions (
    keyspace_name text,
    table_name text,
    top_type text,
    last_update timestamp,
    top frozen<list<frozen<tuple<text, bigint>>>>,
    PRIMARY KEY (keyspace_name, table_name, top_type)
) WITH CLUSTERING ORDER BY (table_name ASC, top_type ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'Stores the top partitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.transferred_ranges (
    operation text,
    keyspace_name text,
    peer inet,
    ranges set<blob>,
    PRIMARY KEY ((operation, keyspace_name), peer)
) WITH CLUSTERING ORDER BY (peer ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'record of transferred ranges for streaming operation'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.transferred_ranges_v2 (
    operation text,
    keyspace_name text,
    peer inet,
    peer_port int,
    ranges set<blob>,
    PRIMARY KEY ((operation, keyspace_name), peer, peer_port)
) WITH CLUSTERING ORDER BY (peer ASC, peer_port ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'record of transferred ranges for streaming operation'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system.view_builds_in_progress (
    keyspace_name text,
    view_name text,
    start_token text,
    end_token text,
    keys_built bigint,
    last_token text,
    PRIMARY KEY (keyspace_name, view_name, start_token, end_token)
) WITH CLUSTERING ORDER BY (view_name ASC, start_token ASC, end_token ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 0
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'views builds current progress'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE KEYSPACE system_distributed WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3' } AND DURABLE_WRITES = true;

CREATE TABLE system_distributed.parent_repair_history (
    parent_id timeuuid,
    columnfamily_names set<text>,
    exception_message text,
    exception_stacktrace text,
    finished_at timestamp,
    keyspace_name text,
    options map<text, text>,
    requested_ranges set<text>,
    started_at timestamp,
    successful_ranges set<text>,
    PRIMARY KEY (parent_id)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'Repair history'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size' : 1, 'compaction_window_unit' : 'DAYS', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 2592000
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_distributed.partition_denylist (
    ks_name text,
    table_name text,
    key blob,
    PRIMARY KEY ((ks_name, table_name), key)
) WITH CLUSTERING ORDER BY (key ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'Partition keys which have been denied access'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_distributed.repair_history (
    keyspace_name text,
    columnfamily_name text,
    id timeuuid,
    coordinator inet,
    coordinator_port int,
    exception_message text,
    exception_stacktrace text,
    finished_at timestamp,
    parent_id timeuuid,
    participants set<inet>,
    participants_v2 set<text>,
    range_begin text,
    range_end text,
    started_at timestamp,
    status text,
    PRIMARY KEY ((keyspace_name, columnfamily_name), id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'Repair history'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size' : 1, 'compaction_window_unit' : 'DAYS', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 2592000
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_distributed.view_build_status (
    keyspace_name text,
    view_name text,
    host_id uuid,
    status text,
    PRIMARY KEY ((keyspace_name, view_name), host_id)
) WITH CLUSTERING ORDER BY (host_id ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 864000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'Materialized View build status'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE KEYSPACE system_schema WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.LocalStrategy' } AND DURABLE_WRITES = true;

CREATE TABLE system_schema.aggregates (
    keyspace_name text,
    aggregate_name text,
    argument_types frozen<list<text>>,
    final_func text,
    initcond text,
    return_type text,
    state_func text,
    state_type text,
    PRIMARY KEY (keyspace_name, aggregate_name, argument_types)
) WITH CLUSTERING ORDER BY (aggregate_name ASC, argument_types ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'user defined aggregate definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.columns (
    keyspace_name text,
    table_name text,
    column_name text,
    clustering_order text,
    column_name_bytes blob,
    kind text,
    position int,
    type text,
    PRIMARY KEY (keyspace_name, table_name, column_name)
) WITH CLUSTERING ORDER BY (table_name ASC, column_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'column definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.dropped_columns (
    keyspace_name text,
    table_name text,
    column_name text,
    dropped_time timestamp,
    kind text,
    type text,
    PRIMARY KEY (keyspace_name, table_name, column_name)
) WITH CLUSTERING ORDER BY (table_name ASC, column_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'dropped column registry'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.functions (
    keyspace_name text,
    function_name text,
    argument_types frozen<list<text>>,
    argument_names frozen<list<text>>,
    body text,
    called_on_null_input boolean,
    language text,
    return_type text,
    PRIMARY KEY (keyspace_name, function_name, argument_types)
) WITH CLUSTERING ORDER BY (function_name ASC, argument_types ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'user defined function definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.indexes (
    keyspace_name text,
    table_name text,
    index_name text,
    kind text,
    options frozen<map<text, text>>,
    PRIMARY KEY (keyspace_name, table_name, index_name)
) WITH CLUSTERING ORDER BY (table_name ASC, index_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'secondary index definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.\"keyspaces\" (
    keyspace_name text,
    durable_writes boolean,
    replication frozen<map<text, text>>,
    PRIMARY KEY (keyspace_name)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'keyspace definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.tables (
    keyspace_name text,
    table_name text,
    additional_write_policy text,
    allow_auto_snapshot boolean,
    bloom_filter_fp_chance double,
    caching frozen<map<text, text>>,
    cdc boolean,
    comment text,
    compaction frozen<map<text, text>>,
    compression frozen<map<text, text>>,
    crc_check_chance double,
    dclocal_read_repair_chance double,
    default_time_to_live int,
    extensions frozen<map<text, blob>>,
    flags frozen<set<text>>,
    gc_grace_seconds int,
    id uuid,
    max_index_interval int,
    memtable text,
    memtable_flush_period_in_ms int,
    min_index_interval int,
    read_repair text,
    read_repair_chance double,
    speculative_retry text,
    PRIMARY KEY (keyspace_name, table_name)
) WITH CLUSTERING ORDER BY (table_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'table definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.triggers (
    keyspace_name text,
    table_name text,
    trigger_name text,
    options frozen<map<text, text>>,
    PRIMARY KEY (keyspace_name, table_name, trigger_name)
) WITH CLUSTERING ORDER BY (table_name ASC, trigger_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'trigger definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.types (
    keyspace_name text,
    type_name text,
    field_names frozen<list<text>>,
    field_types frozen<list<text>>,
    PRIMARY KEY (keyspace_name, type_name)
) WITH CLUSTERING ORDER BY (type_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'user defined type definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

CREATE TABLE system_schema.views (
    keyspace_name text,
    view_name text,
    additional_write_policy text,
    allow_auto_snapshot boolean,
    base_table_id uuid,
    base_table_name text,
    bloom_filter_fp_chance double,
    caching frozen<map<text, text>>,
    cdc boolean,
    comment text,
    compaction frozen<map<text, text>>,
    compression frozen<map<text, text>>,
    crc_check_chance double,
    dclocal_read_repair_chance double,
    default_time_to_live int,
    extensions frozen<map<text, blob>>,
    gc_grace_seconds int,
    id uuid,
    include_all_columns boolean,
    max_index_interval int,
    memtable text,
    memtable_flush_period_in_ms int,
    min_index_interval int,
    read_repair text,
    read_repair_chance double,
    speculative_retry text,
    where_clause text,
    PRIMARY KEY (keyspace_name, view_name)
) WITH CLUSTERING ORDER BY (view_name ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 604800
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'view definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 3600000;

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

/* VIRTUAL KEYSPACE system_virtual_schema WITH REPLICATION = { 'class' : 'null' } AND DURABLE_WRITES = false;*/

/* VIRTUAL TABLE system_virtual_schema.columns (
    keyspace_name text,
    table_name text,
    column_name text,
    clustering_order text,
    column_name_bytes blob,
    kind text,
    position int,
    type text,
    PRIMARY KEY (keyspace_name, table_name, column_name)
)  */

/* VIRTUAL TABLE system_virtual_schema.\"keyspaces\" (
    keyspace_name text,
    PRIMARY KEY (keyspace_name)
)  */

/* VIRTUAL TABLE system_virtual_schema.tables (
    keyspace_name text,
    table_name text,
    comment text,
    PRIMARY KEY (keyspace_name, table_name)
)  */

/* VIRTUAL KEYSPACE system_views WITH REPLICATION = { 'class' : 'null' } AND DURABLE_WRITES = false;*/

/* VIRTUAL TABLE system_views.batch_metrics (
    name text,
    max bigint,
    p50th double,
    p999th double,
    p99th double,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.caches (
    name text,
    capacity_bytes bigint,
    entry_count int,
    hit_count bigint,
    hit_ratio double,
    recent_hit_rate_per_second bigint,
    recent_request_rate_per_second bigint,
    request_count bigint,
    size_bytes bigint,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.clients (
    address inet,
    port int,
    client_options frozen<map<text, text>>,
    connection_stage text,
    driver_name text,
    driver_version text,
    hostname text,
    protocol_version int,
    request_count bigint,
    ssl_cipher_suite text,
    ssl_enabled boolean,
    ssl_protocol text,
    username text,
    PRIMARY KEY (address, port)
)  */

/* VIRTUAL TABLE system_views.coordinator_read_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.coordinator_scan_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.coordinator_write_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.cql_metrics (
    name text,
    value double,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.credentials_cache_keys (
    role text,
    PRIMARY KEY (role)
)  */

/* VIRTUAL TABLE system_views.disk_usage (
    keyspace_name text,
    table_name text,
    mebibytes bigint,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.gossip_info (
    address inet,
    port int,
    dc text,
    dc_version int,
    disk_usage text,
    disk_usage_version int,
    generation int,
    heartbeat int,
    host_id text,
    host_id_version int,
    hostname text,
    internal_address_and_port text,
    internal_address_and_port_version int,
    internal_ip text,
    internal_ip_version int,
    load text,
    load_version int,
    native_address_and_port text,
    native_address_and_port_version int,
    net_version text,
    net_version_version int,
    rack text,
    rack_version int,
    release_version text,
    release_version_version int,
    removal_coordinator text,
    removal_coordinator_version int,
    rpc_address text,
    rpc_address_version int,
    rpc_ready text,
    rpc_ready_version int,
    \"schema\" text,
    schema_version int,
    severity text,
    severity_version int,
    sstable_versions text,
    sstable_versions_version int,
    status text,
    status_version int,
    status_with_port text,
    status_with_port_version int,
    tokens_version int,
    x1 text,
    x10 text,
    x10_version int,
    x1_version int,
    x2 text,
    x2_version int,
    x3 text,
    x3_version int,
    x4 text,
    x4_version int,
    x5 text,
    x5_version int,
    x6 text,
    x6_version int,
    x7 text,
    x7_version int,
    x8 text,
    x8_version int,
    x9 text,
    x9_version int,
    x_11_padding text,
    x_11_padding_version int,
    PRIMARY KEY (address, port)
)  */

/* VIRTUAL TABLE system_views.internode_inbound (
    address inet,
    port int,
    dc text,
    rack text,
    corrupt_frames_recovered bigint,
    corrupt_frames_unrecovered bigint,
    error_bytes bigint,
    error_count bigint,
    expired_bytes bigint,
    expired_count bigint,
    processed_bytes bigint,
    processed_count bigint,
    received_bytes bigint,
    received_count bigint,
    scheduled_bytes bigint,
    scheduled_count bigint,
    throttled_count bigint,
    throttled_nanos bigint,
    using_bytes bigint,
    using_reserve_bytes bigint,
    PRIMARY KEY ((address, port), dc, rack)
)  */

/* VIRTUAL TABLE system_views.internode_outbound (
    address inet,
    port int,
    dc text,
    rack text,
    active_connections bigint,
    connection_attempts bigint,
    error_bytes bigint,
    error_count bigint,
    expired_bytes bigint,
    expired_count bigint,
    overload_bytes bigint,
    overload_count bigint,
    pending_bytes bigint,
    pending_count bigint,
    sent_bytes bigint,
    sent_count bigint,
    successful_connection_attempts bigint,
    using_bytes bigint,
    using_reserve_bytes bigint,
    PRIMARY KEY ((address, port), dc, rack)
)  */

/* VIRTUAL TABLE system_views.jmx_permissions_cache_keys (
    role text,
    PRIMARY KEY (role)
)  */

/* VIRTUAL TABLE system_views.local_read_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.local_scan_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.local_write_latency (
    keyspace_name text,
    table_name text,
    count bigint,
    max_ms double,
    p50th_ms double,
    p99th_ms double,
    per_second double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.max_partition_size (
    keyspace_name text,
    table_name text,
    mebibytes bigint,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.network_permissions_cache_keys (
    role text,
    PRIMARY KEY (role)
)  */

/* VIRTUAL TABLE system_views.pending_hints (
    host_id uuid,
    address inet,
    dc text,
    files int,
    newest timestamp,
    oldest timestamp,
    port int,
    rack text,
    status text,
    PRIMARY KEY (host_id)
)  */

/* VIRTUAL TABLE system_views.permissions_cache_keys (
    role text,
    resource text,
    PRIMARY KEY ((role, resource))
)  */

/* VIRTUAL TABLE system_views.queries (
    thread_id text,
    queued_micros bigint,
    running_micros bigint,
    task text,
    PRIMARY KEY (thread_id)
)  */

/* VIRTUAL TABLE system_views.repair_jobs (
    id uuid,
    completed boolean,
    duration_millis bigint,
    failure_cause text,
    keyspace_name text,
    last_updated_at timestamp,
    participants frozen<list<text>>,
    ranges frozen<list<text>>,
    repair_id timeuuid,
    session_id timeuuid,
    state_failure_timestamp timestamp,
    state_init_timestamp timestamp,
    state_skipped_timestamp timestamp,
    state_snapshot_complete_timestamp timestamp,
    state_snapshot_start_timestamp timestamp,
    state_start_timestamp timestamp,
    state_stream_start_timestamp timestamp,
    state_success_timestamp timestamp,
    state_validation_complete_timestamp timestamp,
    state_validation_start_timestamp timestamp,
    status text,
    success_message text,
    table_name text,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.repair_participates (
    id timeuuid,
    completed boolean,
    duration_millis bigint,
    failure_cause text,
    global boolean,
    incremental boolean,
    initiator text,
    last_updated_at timestamp,
    preview_kind text,
    ranges frozen<list<text>>,
    repaired_at timestamp,
    state_failure_timestamp timestamp,
    state_init_timestamp timestamp,
    state_skipped_timestamp timestamp,
    state_success_timestamp timestamp,
    success_message text,
    tables frozen<set<text>>,
    validations frozen<set<uuid>>,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.repair_sessions (
    id timeuuid,
    completed boolean,
    duration_millis bigint,
    failure_cause text,
    jobs frozen<set<uuid>>,
    keyspace_name text,
    last_updated_at timestamp,
    participants frozen<list<text>>,
    ranges frozen<list<text>>,
    repair_id timeuuid,
    state_failure_timestamp timestamp,
    state_init_timestamp timestamp,
    state_jobs_start_timestamp timestamp,
    state_skipped_timestamp timestamp,
    state_start_timestamp timestamp,
    state_success_timestamp timestamp,
    status text,
    success_message text,
    table_names frozen<list<text>>,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.repair_validations (
    id uuid,
    bytes_read bigint,
    completed boolean,
    duration_millis bigint,
    estimated_partitions bigint,
    estimated_total_bytes bigint,
    failure_cause text,
    initiator text,
    keyspace_name text,
    last_updated_at timestamp,
    partitions_processed bigint,
    progress_percentage float,
    ranges frozen<list<text>>,
    repair_id timeuuid,
    session_id timeuuid,
    state_failure_timestamp timestamp,
    state_init_timestamp timestamp,
    state_sending_trees_timestamp timestamp,
    state_skipped_timestamp timestamp,
    state_start_timestamp timestamp,
    state_success_timestamp timestamp,
    status text,
    success_message text,
    table_name text,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.repairs (
    id timeuuid,
    command_id int,
    completed boolean,
    duration_millis bigint,
    failure_cause text,
    keyspace_name text,
    last_updated_at timestamp,
    options_column_families frozen<set<text>>,
    options_data_centers frozen<set<text>>,
    options_force_repair boolean,
    options_hosts frozen<set<text>>,
    options_ignore_unreplicated_keyspaces boolean,
    options_incremental boolean,
    options_job_threads int,
    options_optimise_streams boolean,
    options_parallelism text,
    options_preview_kind text,
    options_primary_range boolean,
    options_pull_repair boolean,
    options_ranges frozen<set<text>>,
    options_subrange_repair boolean,
    options_trace boolean,
    participants frozen<list<text>>,
    ranges frozen<list<frozen<list<text>>>>,
    sessions frozen<set<timeuuid>>,
    state_failure_timestamp timestamp,
    state_init_timestamp timestamp,
    state_prepare_complete_timestamp timestamp,
    state_prepare_start_timestamp timestamp,
    state_repair_complete_timestamp timestamp,
    state_repair_start_timestamp timestamp,
    state_setup_timestamp timestamp,
    state_skipped_timestamp timestamp,
    state_start_timestamp timestamp,
    state_success_timestamp timestamp,
    status text,
    success_message text,
    table_names frozen<list<text>>,
    type text,
    unfiltered_ranges frozen<list<frozen<list<text>>>>,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.roles_cache_keys (
    role text,
    PRIMARY KEY (role)
)  */

/* VIRTUAL TABLE system_views.rows_per_read (
    keyspace_name text,
    table_name text,
    count bigint,
    max double,
    p50th double,
    p99th double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

/* VIRTUAL TABLE system_views.settings (
    name text,
    value text,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.sstable_tasks (
    keyspace_name text,
    table_name text,
    task_id timeuuid,
    completion_ratio double,
    kind text,
    progress bigint,
    sstables int,
    target_directory text,
    total bigint,
    unit text,
    PRIMARY KEY (keyspace_name, table_name, task_id)
)  */

/* VIRTUAL TABLE system_views.streaming (
    id timeuuid,
    bytes_received bigint,
    bytes_sent bigint,
    bytes_to_receive bigint,
    bytes_to_send bigint,
    duration_millis bigint,
    failure_cause text,
    files_received bigint,
    files_sent bigint,
    files_to_receive bigint,
    files_to_send bigint,
    follower boolean,
    last_updated_at timestamp,
    operation text,
    peers frozen<list<text>>,
    progress_percentage float,
    status text,
    status_failure_timestamp timestamp,
    status_init_timestamp timestamp,
    status_start_timestamp timestamp,
    status_success_timestamp timestamp,
    success_message text,
    PRIMARY KEY (id)
)  */

/* VIRTUAL TABLE system_views.system_properties (
    name text,
    value text,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.thread_pools (
    name text,
    active_tasks int,
    active_tasks_limit int,
    blocked_tasks bigint,
    blocked_tasks_all_time bigint,
    completed_tasks bigint,
    pending_tasks int,
    PRIMARY KEY (name)
)  */

/* VIRTUAL TABLE system_views.tombstones_per_read (
    keyspace_name text,
    table_name text,
    count bigint,
    max double,
    p50th double,
    p99th double,
    PRIMARY KEY ((keyspace_name, table_name))
)  */

CREATE KEYSPACE system_auth WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE TABLE system_auth.network_permissions (
    role text,
    dcs frozen<set<text>>,
    PRIMARY KEY (role)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 7776000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'user network permissions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_auth.resource_role_permissons_index (
    resource text,
    role text,
    PRIMARY KEY (resource, role)
) WITH CLUSTERING ORDER BY (role ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 7776000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'index of db roles with permissions granted on a resource'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_auth.role_members (
    role text,
    member text,
    PRIMARY KEY (role, member)
) WITH CLUSTERING ORDER BY (member ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 7776000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'role memberships lookup table'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_auth.role_permissions (
    role text,
    resource text,
    permissions set<text>,
    PRIMARY KEY (role, resource)
) WITH CLUSTERING ORDER BY (resource ASC)
    AND read_repair = 'BLOCKING'
    AND gc_grace_seconds = 7776000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'permissions granted to db roles'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

CREATE TABLE system_auth.roles (
    role text,
    can_login boolean,
    is_superuser boolean,
    member_of set<text>,
    salted_hash text,
    PRIMARY KEY (role)
) WITH read_repair = 'BLOCKING'
    AND gc_grace_seconds = 7776000
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'role definitions'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 16, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99p'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0
    AND cdc = false
    AND memtable_flush_period_in_ms = 0;

