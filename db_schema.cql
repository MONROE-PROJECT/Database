//DROP KEYSPACE IF EXISTS monroe;

/*CREATE KEYSPACE monroe WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};*/
USE monroe;

CREATE TABLE devices (
    Country      text,
    Site         text,
    NodeId       int,

    Address      text,
    DisplayName  text,
    HostName     text,
    Interfaces   list<text>,
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

CREATE TABLE data_id_to_table (
    DataId      text,
    DataVersion int,

    TableName   text,

    PRIMARY KEY (DataId, DataVersion)
);

CREATE TABLE monroe_exp_ping (
    NodeId         text,
    Timestamp      decimal,
    DataId         text,
    DataVersion    int,
    SequenceNumber bigint,
    
    InterfaceName  text,
    Bytes          int,
    Host           text,
    Rtt            double,

    PRIMARY KEY ((DataVersion, NodeId, InterfaceName), Timestamp)
);

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
    Lac            int,
    Rsrp           int,
    Frequency      int,
    Rsrq           int,
    Band           int,
    Pci            int,
    NwMccMnc       int,
    Rscp           int,
    Rssi           int,

    PRIMARY KEY ((DataVersion, NodeId, InterfaceName), Timestamp)
);

CREATE TABLE monroe_meta_device_gps (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    
    Longitude          decimal,
    Latitude           decimal,
    Altitude           decimal,
    SatelliteCount     int,
    Nmea               text,

    PRIMARY KEY ((DataVersion, NodeId), Timestamp)
);

CREATE TABLE monroe_meta_connectivity (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    
    Iccid              text,
    InterfaceName      text,
    MccMnc             int,
    Mode               int,
    Rssi               int,
    
    PRIMARY KEY ((DataVersion, NodeId, InterfaceName), Timestamp)
);

CREATE TABLE monroe_meta_node_sensor (
    NodeId                 text,
    Timestamp              decimal,
    DataId                 text,
    DataVersion            int,
    SequenceNumber         bigint,
    
    ExperimentsRunning     text,
    
    TempCpu                int,
    
    RestServicesModems     int,
    RestServicesDlb        int,
    RestServicesUsbMonitor int,
    
    SessionId              int,
    SessionStart           int,
    SessionCurrent         int,
    SessionTotal           bigint,
    SessionPercent         decimal,
    
    CpuSystem              decimal,
    CpuSteal               decimal,
    CpuGuest               decimal,
    CpuIoWait              decimal,
    CpuIrq                 decimal,
    CpuNice                decimal,
    CpuIdle                decimal,
    CpuUser                decimal,
    CpuSoftIrq             decimal,
    
    MemoryApps             bigint,
    MemoryFree             bigint,
    MemorySwap             bigint,
    
    PRIMARY KEY ((DataVersion, NodeId), Timestamp)
);

CREATE TABLE monroe_meta_node_event (
    NodeId             text,
    Timestamp          decimal,
    DataId             text,
    DataVersion        int,
    SequenceNumber     bigint,
    EventType          int,
    
    WatchdogFailed     text,
    WatchdogRepaired   text,
    WatchdogStatus     text,
    MaintenanceStart   text,
    MaintenanceStop    text,
    SystemHalt         int,
    SchedulingStarted  int,
    
    PRIMARY KEY ((DataVersion, NodeId), Timestamp)
);
