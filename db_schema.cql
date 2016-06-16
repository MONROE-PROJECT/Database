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

    PRIMARY KEY ((NodeId, Iccid), Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_exp_ping_timestamp on monroe_exp_ping(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

///////////////////////////////////////////////////////////////////////////////
CREATE TABLE monroe_meta_device_modem (
    NodeId         text,
    Timestamp      decimal,
    DataId         text,
    DataVersion    int,
    SequenceNumber bigint,

    InterfaceName  text,
    Cid            int,
    DeviceMode     int,
    DeviceSubmode  int,
    DeviceState    int,
    Ecio           int,
    ENodebId       int,
    Iccid          text,
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

    PRIMARY KEY ((NodeId, Iccid), Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_meta_device_modem_timestamp on monroe_meta_device_modem(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

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

    PRIMARY KEY (NodeId, Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_meta_device_gps_timestamp on monroe_meta_device_gps(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

///////////////////////////////////////////////////////////////////////////////
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

    PRIMARY KEY ((NodeId, Iccid), Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_meta_connectivity_timestamp on monroe_meta_connectivity(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

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
	
	usb0                   text,
	usb0charging           text,
	usb1                   text,
	usb1charging           text,
	usb2                   text,
	usb2charging           text,

    PRIMARY KEY (NodeId, Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_meta_node_sensor_timestamp on monroe_meta_node_sensor(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

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

    PRIMARY KEY (NodeId, SequenceNumber, Timestamp)
);
/*CREATE CUSTOM INDEX i_monroe_meta_node_event_timestamp on monroe_meta_node_event(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/

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

    PRIMARY KEY ((NodeId, Iccid), Timestamp, SequenceNumber)
);
/*CREATE CUSTOM INDEX i_monroe_exp_http on monroe_exp_http(Timestamp) using 'org.apache.cassandra.index.sasi.SASIIndex' with options={'mode':'SPARSE'};*/


CREATE TABLE monroe_exp_http_download (
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

    PRIMARY KEY ((NodeId, Iccid), Timestamp, SequenceNumber)
);

CREATE TABLE monroe_exp_tstat_udp (
    NodeId      text,
    Iccid       text,

    /* We decided to mantain the same formalism of the tstat/mPlane project */
    c_ip        text,
    c_port      int,
    c_first_abs decimal,
    c_durat     decimal,
    c_bytes_all int,
    c_pkts_all  int,
    c_isint     int,
    c_iscrypto  int,
    c_type      int,

    s_ip        text,
    s_port      int,
    s_first_abs decimal,
    s_durat     decimal,
    s_bytes_all int,
    s_pkts_all  int,
    s_isint     int,
    s_iscrypto  int
    s_type      int,

    fqdn        text,

    PRIMARY KEY ((NodeId, Iccid), c_first_abs, c_ip, c_port, s_ip, s_port)
);

CREATE TABLE monroe_exp_tstat_http (
    NodeId            text,
    Iccid             text,

    /* We decided to mantain the same formalism of the tstat/mPlane project */
    c_ip           text,
    c_port            int,
    s_ip              text,
    s_port            int,
    time_abs          decimal,
    method_HTTP       text,
    hostname_response text,
    fqdn_content_len  text,
    path_content_type text,
    referer_server    text,
    user_agent_range  text,
    cookie_location          text,
    dnt_set_cookie          text,
    
    PRIMARY KEY ((NodeId, Iccid), time_abs, c_ip, c_port, s_ip, s_port)
);

CREATE TABLE monroe_exp_tstat_tcp_complete (
    NodeId            text,
    Iccid             text,

    /* We decided to mantain the same formalism of the tstat/mPlane project */
    c_ip              text,
    c_port            int,
    c_pkts_all        int,
    c_rst_cnt         int,
    c_ack_cnt         int,
    c_ack_cnt_p       int,
    c_bytes_uniq      int,
    c_pkts_data       int,
    c_bytes_all       int,
    c_pkts_retx       int,
    c_bytes_retx      int,
    c_pkts_ooo        int,
    c_syn_cnt         int,
    c_fin_cnt         int,
    s_ip              text,
    s_port            int,
    s_pkts_all        int,
    s_rst_cnt         int,
    s_ack_cnt         int,
    s_ack_cnt_p       int,
    s_bytes_uniq      int,
    s_pkts_data       int,
    s_bytes_all       int,
    s_pkts_retx       int,
    s_bytes_retx      int,
    s_pkts_ooo        int,
    s_syn_cnt         int,
    s_fin_cnt         int,
    first             decimal,
    last              decimal,
    durat             decimal,
    c_first           decimal,
    s_first           decimal,
    c_last            decimal,
    s_last            decimal,
    c_first_ack       decimal,
    s_first_ack       decimal,
    c_isint           int,
    s_isint           int,
    c_iscrypto        int,
    s_iscrypto        int,
    con_t             int,
    p2p_t             int,
    http_t            int,
    c_rtt_avg         decimal,
    c_rtt_min         decimal,
    c_rtt_max         decimal,
    c_rtt_std         decimal,
    c_rtt_cnt         decimal,
    c_ttl_min         decimal,
    c_ttl_max         decimal,
    s_rtt_avg         decimal,
    s_rtt_min         decimal,
    s_rtt_max         decimal,
    s_rtt_std         decimal,
    s_rtt_cnt         decimal,
    s_ttl_min         decimal,
    s_ttl_max         decimal,
    p2p_st            int,
    ed2k_data         int,
    ed2k_sig          int,
    ed2k_c2s          int,
    ed2k_c2c          int,
    ed2k_chat         int,
    c_f1323_opt       int,
    c_tm_opt          int,
    c_win_scl         int,
    c_sack_opt        int,
    c_sack_cnt        int,
    c_mss             int,
    c_mss_max         decimal,
    c_mss_min         decimal,
    c_win_max         decimal,
    c_win_min         decimal,
    c_win_0           decimal,
    c_cwin_max        decimal,
    c_cwin_min        decimal,
    c_cwin_ini        int,
    c_pkts_rto        int,
    c_pkts_fs         int,
    c_pkts_reor       int,
    c_pkts_dup        int,
    c_pkts_unk        int,
    c_pkts_fc         int,
    c_pkts_unrto      int,
    c_pkts_unfs       int,
    c_syn_retx        int,
    s_f1323_opt       int,
    s_tm_opt          int,
    s_win_scl         int,
    s_sack_opt        int,
    s_sack_cnt        int,
    s_mss             int,
    s_mss_max         decimal,
    s_mss_min         decimal,
    s_win_max         decimal,
    s_win_min         decimal,
    s_win_0           decimal,
    s_cwin_max        decimal,
    s_cwin_min        decimal,
    s_cwin_ini        int,
    s_pkts_rto        int,
    s_pkts_fs         int,
    s_pkts_reor       int
    s_pkts_dup        int,
    s_pkts_unk        int,
    s_pkts_fc         int,
    s_pkts_unrto      int,
    s_pkts_unfs       int,
    s_syn_retx        int,
    http_req_cnt      int,
    http_res_cnt      int,
    http_res          int,
    c_pkts_push       int,
    s_pkts_push       int,
    c_tls_SNI         text,
    s_tls_SCN         text,
    c_npnalpn         int,
    s_npnalpn         int,
    c_tls_sesid       int,
    c_last_handshakeT decimal,
    s_last_handshakeT decimal,
    c_appdataT        decimal,
    s_appdataT        decimal,
    c_appdataB        int,
    s_appdataB        int,
    fqdn              text,
    dns_rslv          text,
    req_tm            decimal,
    res_tm            decimal,

    PRIMARY KEY ((NodeId, Iccid), first, last, c_ip, c_port, s_ip, s_port)
);

CREATE TABLE monroe_exp_tstat_tcp_nocomplete (
    NodeId       text,
    Iccid        text,

    /* We decided to mantain the same formalism of the tstat/mPlane project */
    c_ip         text,
    c_port       int,
    c_pkts_all   int,
    c_rst_cnt    int,
    c_ack_cnt    int,
    c_ack_cnt_p  int,
    c_bytes_uniq int,
    c_pkts_data  int,
    c_bytes_all  int,
    c_pkts_retx  int,
    c_bytes_retx int,
    c_pkts_ooo   int,
    c_syn_cnt    int,
    c_fin_cnt    int,
    s_ip         text,
    s_port       int,
    s_pkts_all   int,
    s_rst_cnt    int,
    s_ack_cnt    int,
    s_ack_cnt_p  int,
    s_bytes_uniq int,
    s_pkts_data  int,
    s_bytes_all  int,
    s_pkts_retx  int,
    s_bytes_retx int,
    s_pkts_ooo   int,
    s_syn_cnt    int,
    s_fin_cnt    int,
    first        decimal,
    last         decimal,
    durat        decimal,
    c_first      int,
    s_first      int,
    c_last       int,
    s_last       int,
    c_first_ack  int,
    s_first_ack  int,
    c_isint      int,
    s_isint      int,
    c_iscrypto   int,
    s_iscrypto   int,
    con_t        int,
    p2p_t        int,
    http_t       int,

    PRIMARY KEY ((NodeId, Iccid), first, last, c_ip, c_port, s_ip, s_port)
);
