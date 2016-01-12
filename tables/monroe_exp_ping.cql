CREATE TABLE monroe.monroe_exp_ping (
    nodeid text,
    interfacename text,
    timestamp decimal,
    bytes int,
    dataid text,
    dataversion int,
    host text,
    rtt double,
    sequencenumber bigint,
    PRIMARY KEY ((nodeid, interfacename), timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
CREATE INDEX monroe_exp_ping_dataversion_idx ON monroe.monroe_exp_ping (dataversion);
