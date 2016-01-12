CREATE TABLE monroe.devices (
    country text,
    site text,
    nodeid int,
    address text,
    displayname text,
    hostname text,
    interfaces list<text>,
    latitude decimal,
    longitude decimal,
    make text,
    model text,
    modemcount int,
    postcode text,
    sitenote text,
    status text,
    statusstart timestamp,
    usbwificount int,
    validfrom timestamp,
    validto timestamp,
    PRIMARY KEY ((country, site), nodeid)
) WITH CLUSTERING ORDER BY (nodeid ASC)
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
