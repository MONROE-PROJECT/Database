//DROP KEYSPACE IF EXISTS monroe;

/*CREATE KEYSPACE monroe WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};*/
USE monroe;

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE devices (
    Country      text,
    Site         text,
    NodeId       int,

    Address      text,
    DisplayName  text,
    HostName     text,
    Interfaces   list<text>,
	IfDetails    map<text, text>,
    Latitude     decimal,
    Longitude    decimal,
    Make         text,
    Model        text,
    ModemCount   int,
    PostCode     text,
    SiteNote     text,
    Status       text,
    StatusStart  timestamp,
    UsbWifiCount int,
    ValidFrom    timestamp,
    ValidTo      timestamp,

    PRIMARY KEY ((Country, Site), NodeId)
);
CREATE CUSTOM INDEX i_devices_nodeid on devices(NodeId) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE data_id_to_table (
    DataId      text,
    DataVersion int,

    TableName   text,

    PRIMARY KEY (DataId, DataVersion)
);

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_exp_ping (
    NodeId         text,
	Guid           text,
    Timestamp      decimal,
	SequenceNumber bigint,
    DataId         text,
    DataVersion    int,
    
	Operator       text,
	Iccid          text,

    Bytes          int,
    Host           text,
    Rtt            double,

    PRIMARY KEY (NodeId, Iccid)
);
CREATE CUSTOM INDEX i_monroe_exp_ping_timestamp on monroe_exp_ping(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_device_modem (
    NodeId         text,
    Timestamp      decimal,
    DataId         text,
    DataVersion    int,
    SequenceNumber bigint,
    
    Cid            int,
    DeviceMode     int,
    DeviceSubmode  int,
    DeviceState    int,
    Ecio           int,
    ENodebId       int,
    Iccid          text,
    InterfaceName  text,
    Imsi           text,
    ImsiMccMnc     int,
    Imei           text,
    IpAddress      text,
    InternalIpAddress text,
    MccMnc         int,
    Operator       text,
    Lac            int,
    Rsrp           int,
    Frequency      int,
    Rsrq           int,
    Band           int,
    Pci            int,
    NwMccMnc       int,
    Rscp           int,
    Rssi           int,

    PRIMARY KEY (NodeId, Iccid)
);
CREATE CUSTOM INDEX i_monroe_meta_device_modem_timestamp on monroe_meta_device_modem(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_device_gps (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    
    Longitude          decimal,
    Latitude           decimal,
    Altitude           decimal,
    Speed              decimal,
    SatelliteCount     int,
    Nmea               text,

    PRIMARY KEY (NodeId)
);
CREATE CUSTOM INDEX i_monroe_meta_device_gps_timestamp on monroe_meta_device_gps(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_connectivity (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    
    Iccid              text,
    InterfaceId        text,
    MccMnc             int,
    Mode               int,
    Rssi               int,
    
    PRIMARY KEY (NodeId, InterfaceId)
);
CREATE CUSTOM INDEX i_monroe_meta_connectivity_timestamp on monroe_meta_connectivity(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_node_sensor (
    NodeId                 text,
    Timestamp              decimal,
    DataId                 text,
    DataVersion            int,
    SequenceNumber         bigint,
    
    Running                text,
    
    Cpu                    text,
    
    Modems                 text,
    Dlb                    text,
    UsbMonitor             text,
    
    Id                     text,
    Start                  text,
    Current                text,
    Total                  text,
    Percent                text,
    
    System                 text,
    Steal                  text,
    Guest                  text,
    IoWait                 text,
    Irq                    text,
    Nice                   text,
    Idle                   text,
    User                   text,
    SoftIrq                text,
    
    Apps                   text,
    Free                   text,
    Swap                   text,
    
    PRIMARY KEY (NodeId)
);
CREATE CUSTOM INDEX i_monroe_meta_node_sensor_timestamp on monroe_meta_node_sensor(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_node_event (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    
    EventType          text,
    Message            text,
    User               text,
    
    //WatchdogFailed     text,
    //WatchdogRepaired   text,
    //WatchdogStatus     text,
    //MaintenanceStart   text,
    //MaintenanceStop    text,
    //SystemHalt         int,
    //SchedulingStarted  int,
    
    PRIMARY KEY (NodeId)
);
CREATE CUSTOM INDEX i_monroe_meta_node_event_timestamp on monroe_meta_node_event(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_exp_http (
    NodeId         text,
	Guid           text,
    Timestamp      decimal,
	SequenceNumber bigint,
    DataId         text,
    DataVersion    int,
    
	Operator       text,
	Iccid          text,

	TotalTime      double,
    Bytes          int,
	SetupTime      double,
	DownloadTime   double,
    Host           text,
	Speed          double,
    Port           text,

    PRIMARY KEY (NodeId, Iccid)
);
CREATE CUSTOM INDEX i_monroe_exp_http on monroe_exp_http(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};
