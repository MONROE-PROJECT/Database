DROP KEYSPACE IF EXISTS monroe;

CREATE KEYSPACE monroe WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
USE monroe;

CREATE TABLE devices (
    country      text,
    site         text,
    nodeid       int,

    address      text,
    displayname  text,
    hostname     text,
    interfaces   list<text>,
    latitude     decimal,
    longitude    decimal,
    make         text,
    model        text,
    modemcount   int,
    postcode     text,
    sitenote     text,
    status       text,
    statusstart  timestamp,
    usbwificount int,
    validfrom    timestamp,
    validto      timestamp,

    PRIMARY KEY ((country, site), nodeid)
);

CREATE TABLE data_id_to_table (
    dataid      text,
    dataversion int,

    tablename   text,

    PRIMARY KEY (dataid, dataversion)
);

CREATE TABLE monroe_exp_ping (
    dataversion    int,
    nodeid         text,
    interfacename  text,
    timestamp      decimal,

    bytes          int,
    dataid         text,
    host           text,
    rtt            double,
    sequencenumber bigint,

    PRIMARY KEY ((dataversion, nodeid, interfacename), timestamp)
);
