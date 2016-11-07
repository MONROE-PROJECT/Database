#!/usr/bin/python

"""
 Example tool to show access to the MONROE project Cassandra database tables from Python.
  https://www.monroe-project.eu
  Creator: Miguel PeÃn QuirÃs, IMDEA Networks Institute
  mikepeon@imdea.org

 The method shown here is the easiest, but the least efficient because it runs in a single
  thread and stops to fetch the next page of rows. More efficient options are asynchronous
  retrieval of rows (in parallel with the processing of the previous page), multithreading
  or multiprocessing (e.g., one process per query).

 One connection per process. If using fork(), remember not to reuse the same connection from the
  child process.

 Dependencies: sudo pip install cassandra-driver python-dateutil
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from time import struct_time, strftime, gmtime
from datetime import datetime
from calendar import timegm
from dateutil.relativedelta import relativedelta

def FileNamePrefix(startTime):
	# Returns a date-stamped file name prefix including path.
	return "/experiments/dailyDumps/{}_".format(strftime("%Y-%m-%d", gmtime(startTime)))

def CalcDumpTimes(daysBack):
	# Get current time and build a timestamp for 00:00 of the PREVIOUS day.
	t = (datetime.utcnow() + relativedelta(days=-daysBack)).utctimetuple()
	s = struct_time(tuple([t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0]))
	startTime = timegm(s)
	return (startTime, startTime + 3600*24)

def FormatDate():
	return "[{} (UTC)] --".format(datetime.utcnow())


def DumpOneDay(session, daysBack):
	(startTime, endTime) = CalcDumpTimes(daysBack)
	print "\n======================================================================"
	print "======================================================================"
	print "======================================================================"
	print FormatDate(), "Dumping MONROE tables for interval [{}, {})\n".format(startTime, endTime)
	
	########## monroe_exp_ping ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_ping.csv".format(startTime)
	with open(fileName, "wt") as output:	
		output.write("nodeid,iccid,timestamp,sequencenumber,bytes,dataid,dataversion,guid,host,operator,rtt\n")
		query = "select * from monroe_exp_ping where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0;
		for row in rows:
			# The driver automatically fetches the next page of rows when the current one is exhausted.
			try:
				output.write("{},{},{},{},{},{},{},{},{},{},{}\n".format(row.nodeid, row.iccid, row.timestamp, row.sequencenumber, row.bytes, row.dataid, row.dataversion, row.guid, row.host, row.operator.encode('latin-1'), row.rtt))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)


	########## monroe_meta_connectivity ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_meta_connectivity.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid,iccid,timestamp,sequencenumber,dataid,dataversion,interfacename,mccmnc,mode,rssi\n")
		query = "select * from monroe_meta_connectivity where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write("{},{},{},{},{},{},{},{},{},{}\n".format(row.nodeid, row.iccid, row.timestamp, row.sequencenumber, row.dataid, row.dataversion, row.interfacename, row.mccmnc, row.mode, row.rssi))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)
	
	########## monroe_exp_http_download ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_http_download.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid,iccid,timestamp,sequencenumber,bytes,dataid,dataversion,downloadtime,guid,host,operator,port,setuptime,speed,totaltime\n")
		query = "select * from monroe_exp_http_download where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(row.nodeid, row.iccid, row.timestamp, row.sequencenumber, row.bytes, row.dataid, row.dataversion, row.downloadtime, row.guid, row.host, row.operator, row.port, row.setuptime, row.speed, row.totaltime))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_meta_device_gps #################
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_meta_device_gps.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid,timestamp,sequencenumber,altitude,dataid,dataversion,latitude,longitude,nmea,satellitecount,speed\n")
		query = "select * from monroe_meta_device_gps where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				nmea = row.nmea.replace("\r","\\r").replace("\n","\\n")
				output.write('{},{},{},{},{},{},{},{},"{}",{},{}\n'.format(row.nodeid, row.timestamp, row.sequencenumber, row.altitude, row.dataid, row.dataversion, row.latitude, row.longitude, nmea, row.satellitecount, row.speed))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_meta_device_modem ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_meta_device_modem.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid,iccid,timestamp,sequencenumber,band,cid,dataid,dataversion,devicemode,devicestate,devicesubmode,ecio,enodebid,frequency,imei,imsi,imsimccmnc,interfacename,internalinterface,internalipaddress,ipaddress,lac,mccmnc,nwmccmnc,operator,pci,rscp,rsrp,rsrq,rssi\n")
		query = "select * from monroe_meta_device_modem where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(row.nodeid, row.iccid, row.timestamp, row.sequencenumber, row.band, row.cid, row.dataid, row.dataversion, row.devicemode, row.devicestate, row.devicesubmode, row.ecio, row.enodebid, row.frequency, row.imei, row.imsi, row.imsimccmnc, row.interfacename, row.internalinterface, row.internalipaddress, row.ipaddress, row.lac, row.mccmnc, row.nwmccmnc, row.operator.encode('latin-1'), row.pci, row.rscp, row.rsrp, row.rsrq, row.rssi))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_meta_node_event ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_meta_node_event.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid, sequencenumber, timestamp, dataid, dataversion, eventtype, message, user\n")
		query = "select * from monroe_meta_node_event where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},"{}",{}\n'.format(row.nodeid, row.sequencenumber, row.timestamp, row.dataid, row.dataversion, row.eventtype, row.message, row.user))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_meta_node_sensor ###############
	session.default_fetch_size = 10
	fileName = FileNamePrefix(startTime) + "{}_monroe_meta_node_sensor.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("nodeid, timestamp, sequencenumber, apps, cpu, current, dataid, dataversion, dlb, free, guest, id, idle, iowait, irq, modems, nice, percent, running, softirq, start, steal, swap, system, total, usb0, usb0charging, usb1, usb1charging, usb2, usb2charging, usbmonitor, user\n")
		query = "select * from monroe_meta_node_sensor where timestamp >= {} and timestamp < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(row.nodeid, row.timestamp, row.sequencenumber, row.apps, row.cpu, row.current, row.dataid, row.dataversion, row.dlb, row.free, row.guest, row.id, row.idle, row.iowait, row.irq, row.modems, row.nice, row.percent, row.running, row.softirq, row.start, row.steal, row.swap, row.system, row.total, row.usb0, row.usb0charging, row.usb1, row.usb1charging, row.usb2, row.usb2charging, row.usbmonitor, row.user))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_simple_traceroute ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_simple_traceroute.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId\ttimestamp\tendTime\tDataId\tDataVersion\tcontainerTimestamp\thop\ttargetdomainname\tInterfaceName\tIpDst\tnumberOfHops\tsizeOfProbes\tIP\tHopName\tRTTSection\tannotationSection\n")
		query = "select * from monroe_exp_simple_traceroute where timestamp >= {} and timestamp < {}".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(row.nodeid, row.timestamp, row.endtime, row.dataid, row.dataversion, row.containertimestamp, row.hop, row.targetdomainname, row.interfacename, row.ipdst, row.numberofhops, row.sizeofprobes, row.ip, row.hopname, row.rttsection, row.annotationsection))
			except Exception as error:
                                print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_exhaustive_paris ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_exhaustive_paris.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId\ttimestamp\tendTime\tDataId\tDataVersion\tcontainerTimestamp\thop\ttargetdomainname\tInterfaceName\tIpDst\tPortDst\tIpSrc\tPortSrc\tIP\tProto\tAlgorithm\tduration\tMinHopRTT\tMedianHopRTT\tMaxHopRTT\tStdHopRTT\tannotation\tflowIds\tMPLS\tTransmittedProbes\tSuccessfulProbes\n")
		query = "select * from monroe_exp_exhaustive_paris where timestamp >= {} and timestamp < {}".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(row.nodeid, row.timestamp, row.endtime, row.dataid, row.dataversion, row.containertimestamp, row.hop, row.targetdomainname, row.interfacename, row.ipdst, row.portdst, row.ipsrc, row.portsrc, row.ip, row.proto, row.algorithm, row.duration, row.minhoprtt, row.medianhoprtt, row.maxhoprtt, row.stdhoprtt, row.annotation, row.flowids, row.mpls, row.transmittedprobes, row.successfulprobes))
			except Exception as error:
				print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_tstat_udp_complete ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_tstat_udp_complete.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId,Iccid,DataId,c_ip,c_port,c_first_abs,c_durat,c_bytes_all,c_pkts_all,c_isint,c_iscrypto,c_type,s_ip,s_port,s_first_abs,s_durat,s_bytes_all,s_pkts_all,s_isint,s_iscrypto,s_type,fqdn\n")
		query = "select * from monroe_exp_tstat_udp_complete where c_first_abs >= {} and c_first_abs < {} allow filtering".format(startTime*1000, endTime*1000)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(row.nodeid, row.iccid, row.dataid, row.c_ip, row.c_port, row.c_first_abs, row.c_durat, row.c_bytes_all, row.c_pkts_all, row.c_isint, row.c_iscrypto, row.c_type, row.s_ip, row.s_port, row.s_first_abs, row.s_durat, row.s_bytes_all, row.s_pkts_all, row.s_isint, row.s_iscrypto, row.s_type, row.fqdn))
			except Exception as error:
				print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_tstat_http_complete ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_tstat_http_complete.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId,Iccid,DataId,c_ip,c_port,s_ip,s_port,time_abs,method_HTTP,hostname_response,fqdn_content_len,path_content_type,referer_server,user_agent_range,cookie_location,dnt_set_cookie\n")
		query = "select * from monroe_exp_tstat_http_complete where time_abs >= {} and time_abs < {} allow filtering".format(startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(row.nodeid, row.iccid, row.dataid, row.c_ip, row.c_port, row.s_ip, row.s_port, row.time_abs, row.method_http, row.hostname_response, row.fqdn_content_len, row.path_content_type, row.referer_server, row.user_agent_range, row.cookie_location, row.dnt_set_cookie))
			except Exception as error:
				print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_tstat_tcp_complete ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_tstat_tcp_complete.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId,Iccid,DataId,c_ip,c_port,c_pkts_all,c_rst_cnt,c_ack_cnt,c_ack_cnt_p,c_bytes_uniq,c_pkts_data,c_bytes_all,c_pkts_retx,c_bytes_retx,c_pkts_ooo,c_syn_cnt,c_fin_cnt,s_ip,s_port,s_pkts_all,s_rst_cnt,s_ack_cnt,s_ack_cnt_p,s_bytes_uniq,s_pkts_data,s_bytes_all,s_pkts_retx,s_bytes_retx,s_pkts_ooo,s_syn_cnt,s_fin_cnt,first,last,durat,c_first,s_first,c_last,s_last,c_first_ack,s_first_ack,c_isint,s_isint,c_iscrypto,s_iscrypto,con_t,p2p_t,http_t,c_rtt_avg,c_rtt_min,c_rtt_max,c_rtt_std,c_rtt_cnt,c_ttl_min,c_ttl_max,s_rtt_avg,s_rtt_min,s_rtt_max,s_rtt_std,s_rtt_cnt,s_ttl_min,s_ttl_max,p2p_st,ed2k_data,ed2k_sig,ed2k_c2s,ed2k_c2c,ed2k_chat,c_f1323_opt,c_tm_opt,c_win_scl,c_sack_opt,c_sack_cnt,c_mss,c_mss_max,c_mss_min,c_win_max,c_win_min,c_win_0,c_cwin_max,c_cwin_min,c_cwin_ini,c_pkts_rto,c_pkts_fs,c_pkts_reor,c_pkts_dup,c_pkts_unk,c_pkts_fc,c_pkts_unrto,c_pkts_unfs,c_syn_retx,s_f1323_opt,s_tm_opt,s_win_scl,s_sack_opt,s_sack_cnt,s_mss,s_mss_max,s_mss_min,s_win_max,s_win_min,s_win_0,s_cwin_max,s_cwin_min,s_cwin_ini,s_pkts_rto,s_pkts_fs,s_pkts_reor,s_pkts_dup,s_pkts_unk,s_pkts_fc,s_pkts_unrto,s_pkts_unfs,s_syn_retx,http_req_cnt,http_res_cnt,http_res,c_pkts_push,s_pkts_push,c_tls_SNI,s_tls_SCN,c_npnalpn,s_npnalpn,c_tls_sesid,c_last_handshakeT,s_last_handshakeT,c_appdataT,s_appdataT,c_appdataB,s_appdataB,fqdn,dns_rslv,req_tm,res_tm\n")
		query = "select * from monroe_exp_tstat_tcp_complete where first >= {} and first < {} allow filtering".format(startTime*1000, endTime*1000)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(row.nodeid, row.iccid, row.dataid, row.c_ip, row.c_port, row.c_pkts_all, row.c_rst_cnt, row.c_ack_cnt, row.c_ack_cnt_p, row.c_bytes_uniq, row.c_pkts_data, row.c_bytes_all, row.c_pkts_retx, row.c_bytes_retx, row.c_pkts_ooo, row.c_syn_cnt, row.c_fin_cnt, row.s_ip, row.s_port, row.s_pkts_all, row.s_rst_cnt, row.s_ack_cnt, row.s_ack_cnt_p, row.s_bytes_uniq, row.s_pkts_data, row.s_bytes_all, row.s_pkts_retx, row.s_bytes_retx, row.s_pkts_ooo, row.s_syn_cnt, row.s_fin_cnt, row.first, row.last, row.durat, row.c_first, row.s_first, row.c_last, row.s_last, row.c_first_ack, row.s_first_ack, row.c_isint, row.s_isint, row.c_iscrypto, row.s_iscrypto, row.con_t, row.p2p_t, row.http_t, row.c_rtt_avg, row.c_rtt_min, row.c_rtt_max, row.c_rtt_std, row.c_rtt_cnt, row.c_ttl_min, row.c_ttl_max, row.s_rtt_avg, row.s_rtt_min, row.s_rtt_max, row.s_rtt_std, row.s_rtt_cnt, row.s_ttl_min, row.s_ttl_max, row.p2p_st, row.ed2k_data, row.ed2k_sig, row.ed2k_c2s, row.ed2k_c2c, row.ed2k_chat, row.c_f1323_opt, row.c_tm_opt, row.c_win_scl, row.c_sack_opt, row.c_sack_cnt, row.c_mss, row.c_mss_max, row.c_mss_min, row.c_win_max, row.c_win_min, row.c_win_0, row.c_cwin_max, row.c_cwin_min, row.c_cwin_ini, row.c_pkts_rto, row.c_pkts_fs, row.c_pkts_reor, row.c_pkts_dup, row.c_pkts_unk, row.c_pkts_fc, row.c_pkts_unrto, row.c_pkts_unfs, row.c_syn_retx, row.s_f1323_opt, row.s_tm_opt, row.s_win_scl, row.s_sack_opt, row.s_sack_cnt, row.s_mss, row.s_mss_max, row.s_mss_min, row.s_win_max, row.s_win_min, row.s_win_0, row.s_cwin_max, row.s_cwin_min, row.s_cwin_ini, row.s_pkts_rto, row.s_pkts_fs, row.s_pkts_reor, row.s_pkts_dup, row.s_pkts_unk, row.s_pkts_fc, row.s_pkts_unrto, row.s_pkts_unfs, row.s_syn_retx, row.http_req_cnt, row.http_res_cnt, row.http_res, row.c_pkts_push, row.s_pkts_push, row.c_tls_sni, row.s_tls_scn, row.c_npnalpn, row.s_npnalpn, row.c_tls_sesid, row.c_last_handshaket, row.s_last_handshaket, row.c_appdatat, row.s_appdatat, row.c_appdatab, row.s_appdatab, row.fqdn, row.dns_rslv, row.req_tm, row.res_tm))
			except Exception as error:
				print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

	########## monroe_exp_tstat_tcp_nocomplete ###############
	session.default_fetch_size = 1000
	fileName = FileNamePrefix(startTime) + "{}_monroe_exp_tstat_tcp_nocomplete.csv".format(startTime)
	with open(fileName, "wt") as output:
		output.write("NodeId,Iccid,DataId,c_ip,c_port,c_pkts_all,c_rst_cnt,c_ack_cnt,c_ack_cnt_p,c_bytes_uniq,c_pkts_data,c_bytes_all,c_pkts_retx,c_bytes_retx,c_pkts_ooo,c_syn_cnt,c_fin_cnt,s_ip,s_port,s_pkts_all,s_rst_cnt,s_ack_cnt,s_ack_cnt_p,s_bytes_uniq,s_pkts_data,s_bytes_all,s_pkts_retx,s_bytes_retx,s_pkts_ooo,s_syn_cnt,s_fin_cnt,first,last,durat,c_first,s_first,c_last,s_last,c_first_ack,s_first_ack,c_isint,s_isint,c_iscrypto,s_iscrypto,con_t,p2p_t,http_t\n")
		query = "select * from monroe_exp_tstat_tcp_nocomplete where first >= {} and first < {} allow filtering".format(startTime*1000, endTime*1000)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				output.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(row.nodeid, row.iccid, row.dataid, row.c_ip, row.c_port, row.c_pkts_all, row.c_rst_cnt, row.c_ack_cnt, row.c_ack_cnt_p, row.c_bytes_uniq, row.c_pkts_data, row.c_bytes_all, row.c_pkts_retx, row.c_bytes_retx, row.c_pkts_ooo, row.c_syn_cnt, row.c_fin_cnt, row.s_ip, row.s_port, row.s_pkts_all, row.s_rst_cnt, row.s_ack_cnt, row.s_ack_cnt_p, row.s_bytes_uniq, row.s_pkts_data, row.s_bytes_all, row.s_pkts_retx, row.s_bytes_retx, row.s_pkts_ooo, row.s_syn_cnt, row.s_fin_cnt, row.first, row.last, row.durat, row.c_first, row.s_first, row.c_last, row.s_last, row.c_first_ack, row.s_first_ack, row.c_isint, row.s_isint, row.c_iscrypto, row.s_iscrypto, row.con_t, row.p2p_t, row.http_t))
			except Exception as error:
				print "Error in row:", row, error
			count += 1
	print FormatDate(), "Dumped {} rows to {}\n".format(count, fileName)

if __name__ == '__main__':

	auth = PlainTextAuthProvider(username = "YYYYYY", password = "XXXXXX")
	cluster = Cluster(['127.0.0.1'], auth_provider = auth)
	session = None
	session = cluster.connect("monroe") # Set default keyspace to 'monroe'
	session.default_timeout = None
	session.default_fetch_size = 1000

	for ii in range (1, 2): # Default is one day back (the previous day).
		DumpOneDay(session, ii)

	cluster.shutdown() # Closes connection to the DB and frees resources.

	print FormatDate(), "DUMP FINISHED.\n"

