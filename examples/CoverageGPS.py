#!/usr/bin/python

"""
 Example tool to correlate modem connection modes with GPS positions for a node in a time interval.
  Output is in KML format for Google Earth.
  https://www.monroe-project.eu
  Creator: Miguel Peon Quiros, IMDEA Networks Institute
  mikepeon@imdea.org

 Dependencies: sudo pip install cassandra-driver python-dateutil

 Cassandra driver (Python) documentation: https://datastax.github.io/python-driver/index.html
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from time import struct_time, strftime, gmtime
from datetime import datetime
from calendar import timegm
from dateutil.relativedelta import relativedelta
from decimal import *
import argparse

def DumpKML(nodeID, startTime, endTime, entries):
	fileName = "{}_{}_{}.kml".format(nodeID, startTime, endTime)
	with open(fileName, "wt") as output:
                # Write KML file headers.
                output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\">\n"
                                "<Document>\n"
                                "<Style id=\"IconUnknown\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_Unknown.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
                                "<Style id=\"IconDisconnected\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_Disconnected.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
                                "<Style id=\"IconNoService\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_NoService.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
                                "<Style id=\"Icon2G\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_2G.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
                                "<Style id=\"Icon3G\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_3G.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
                                "<Style id=\"IconLTE\">\n"
				"  <IconStyle>\n"
				"    <scale>0.5</scale>\n"
				"    <Icon>\n"
				"      <href>Mark_LTE.png</href>\n"
				"    </Icon>\n"
				"  </IconStyle>\n"
				"</Style>\n"
				"<Folder>\n")

		count = 0
		for entry in entries:
			output.write("\n<Placemark>\n"
				"<description>{}</description>\n"
				"<styleUrl>{}</styleUrl>\n"
				"<Point> <coordinates>{},{},{} </coordinates> </Point>\n"
				"</Placemark>\n".format(
					entry['description'], entry['iconStyle'], entry['longitude'],
					entry['latitude'], entry['altitude']))
			count += 1

                # Write KML file footers.
		output.write("</Folder>\n</Document>\n</kml>\n")

	print "Dumped {} positions to {}\n".format(count, fileName)


###############################################################################
# Returns a list of GPS positions as read from the query rows.
def FetchPositions(session, startTime, endTime, nodeID):
	print "Extracting GPS positions for node {} during interval [{}, {})".format(nodeID, startTime, endTime)
	
	session.default_fetch_size = 1000
	gps = []
	query = "select nmea, nodeid, timestamp, latitude, longitude, altitude, speed, satellitecount from monroe_meta_device_gps where nodeid='{}' and timestamp >= {} and timestamp < {} order by timestamp asc".format(nodeID, startTime, endTime)
	print query
	rows = session.execute(query, timeout = None)
	count = 0
	for row in rows:
		try:
			if row.nmea.find("GPRMC") != -1:
				gps.append(row)
				count += 1
		except Exception as error:
			print "Error in row:", row, error
	print "Read {} GPS positions\n".format(count)
	return gps


###############################################################################
# Returns a list of modem statuses as read from the query rows.
def FetchModemStatus(session, startTime, endTime, nodeID, iccids, operator):
	print "Extracting modem status for node {} during interval [{}, {})".format(nodeID, startTime, endTime)
	
	session.default_fetch_size = None
	modem = []
	query = "select nodeid,iccid,timestamp,band,devicemode,devicestate,devicesubmode,frequency,interfacename,internalinterface,lac,operator,pci,rscp,rsrp,rsrq,rssi from monroe_meta_device_modem where nodeid='{}' and iccid in ('{}') and timestamp >= {} and timestamp < {} order by timestamp asc".format(nodeID, "','".join(iccids), startTime, endTime)
	print query
	rows = session.execute(query, timeout = None)
	count = 0
	for row in rows:
		try:
			if (operator == None) or (operator == row.operator):
				modem.append(row)
				count += 1
		except Exception as error:
			print "Error in row:", row, error
	print "Read {} modem statuses\n".format(count)
	return modem


###############################################################################
# Returns the list of iccids associated to the given nodeID
def FetchNodeICCIDs(session, nodeID):
	print "Extracting ICCIDs for node {}".format(nodeID)
	
	session.default_fetch_size = None
	query = "select interfaces from devices where nodeid={}".format(nodeID)
	print query
	rows = session.execute(query, timeout = None)
	try:
		iccids = rows[0].interfaces
	except Exception as error:
		print "Error in row:", rows[0], error
	return iccids


###############################################################################
#  Traverses a list of GPS positions and a list of modem statuses and combines
# them based on timestamp.
#  Returns a list of position-modem status.
def TraverseGPSAndModem(gps, modem, minGPSInterval):
	combinedEntries = []
	lastModem = None
	lastGPS = None
	lastTimestamp = 0
	iModem = 0
	iGPS = 0
	while (iModem < len(modem)) and (iGPS < len(gps)):	# TODO Change to or and check for buffer overruns.
		filteredPos = False	# Marks whether we are skipping a GPS position or not.
		# Update the last known GPS and modem entries.
		if (gps[iGPS].timestamp == modem[iModem].timestamp):
			lastGPS = gps[iGPS]
			lastModem = modem[iModem]
			lastTimestamp = lastGPS.timestamp
			iGPS += 1
			iModem += 1
		elif (gps[iGPS].timestamp < modem[iModem].timestamp):
			if (gps[iGPS].timestamp >= (lastTimestamp + minGPSInterval)):
				lastGPS = gps[iGPS]
				lastTimestamp = lastGPS.timestamp
			else:
				filteredPos = True
			iGPS += 1
		else:	# gps[iGPS].timestamp > modem[iModem].timestamp
			lastModem = modem[iModem]
			lastTimestamp = lastModem.timestamp
			iModem += 1

		# Create a new entry with the last known GPS and modem values
		if (lastGPS != None) and (lastModem != None) and not filteredPos:
			iconStyle = "#IconUnknown" if lastModem.devicemode == 0 else "#IconDisconnected" if lastModem.devicemode == 1 else "#IconNoService" if lastModem.devicemode == 2 else "#Icon2G" if lastModem.devicemode == 3 else "#Icon3G" if lastModem.devicemode == 4 else "#IconLTE" if lastModem.devicemode == 5 else "#IconUnknown"
			try:
				combinedEntries.append(
					{'description': 
						"Node: {}\nTimestamp: {}\nLatitude: {} {}\nLongitude: {} {}\nAltitude: {}\n"
						"Speed: {} Km/h\nSatellites: {}\n"
						"Modem mode: {}\nModem submode: {}\n"
						"ICCID: {}\nBand: {}\nDeviceState: {}\n"
						"Frequency: {}\nInterfaceName: {}\nInternalInterface: {}\n"
						"LAC: {}\nOperator: {}\nPCI: {}\nRSCP: {}\nRSRP: {}\nRSSI: {}".format(
							lastGPS.nodeid, lastGPS.timestamp,						
                        	                	lastGPS.latitude, 'N' if lastGPS.latitude >= 0.0 else 'S',
                                	        	lastGPS.longitude, 'E' if lastGPS.longitude >= 0.0 else 'W',
                                        		lastGPS.altitude, 
							lastGPS.speed * Decimal('1.852') if lastGPS.speed != None else lastGPS.speed,
							lastGPS.satellitecount,
							"Unknown (0)" if lastModem.devicemode == 0 
								else "Disconnected (1)" if lastModem.devicemode == 1 
								else "No service (2)" if lastModem.devicemode == 2 
								else "2G (3)" if lastModem.devicemode == 3 
								else "3G (4)" if lastModem.devicemode == 4 
								else "LTE (5)" if lastModem.devicemode == 5 
								else "None" if lastModem.devicemode == None 
								else "?? ({})".format(lastModem.devicemode),
							"Unknown (0)" if lastModem.devicesubmode == 0 
								else "UMTS (1)" if lastModem.devicesubmode == 1 
								else "WCDMA (2)" if lastModem.devicesubmode == 2 
								else "EVDO (3)" if lastModem.devicesubmode == 3 
								else "HSPA (4)" if lastModem.devicesubmode == 4 
								else "HSPA+ (5)" if lastModem.devicesubmode == 5 
								else "DC HSPA (6)" if lastModem.devicesubmode == 6 
								else "DC HSPA+ (7)" if lastModem.devicesubmode == 7 
								else "HSDPA (8)" if lastModem.devicesubmode == 8 
								else "HSUPA (9)" if lastModem.devicesubmode == 9 
								else "HSDPA+HSUPA (10)" if lastModem.devicesubmode == 10 
								else "HSDPA+ (11)" if lastModem.devicesubmode == 11 
								else "HSDPA+HSUPA (12)" if lastModem.devicesubmode == 12 
								else "DC HSDPA+ (13)" if lastModem.devicesubmode == 13 
								else "DC HSDPA + HSUPA (14)" if lastModem.devicesubmode == 14 
								else "None" if lastModem.devicesubmode == None 
								else "?? ({})".format(lastModem.devicesubmode),
							lastModem.iccid, lastModem.band, 
							"Unknown (0)" if lastModem.devicestate == 0 
								else "Registered (1)" if lastModem.devicestate == 1 
								else "Unregistered (2)" if lastModem.devicestate == 2 
								else "Connected (3)" if lastModem.devicestate == 3 
								else "Disconnected (4)" if lastModem.devicestate == 4 
								else "None" if lastModem.devicestate == None 
								else "?? ({})".format(lastModem.devicestate),
							lastModem.frequency, 
							lastModem.interfacename, lastModem.internalinterface, lastModem.lac,
							lastModem.operator.encode('latin-1'), lastModem.pci, lastModem.rscp, lastModem.rsrp,
							lastModem.rssi),
					'iconStyle': iconStyle,
					'longitude': lastGPS.longitude,
					'latitude': lastGPS.latitude,
					'altitude': lastGPS.altitude
					} )
			except Exception as error:
				print "-------------------- EXCEPTION: ", error
				print lastGPS
				print lastModem
				print "--------------------"
	return combinedEntries


###############################################################################
def ParseCommandLine():
	parser = argparse.ArgumentParser(description = "Modem status - GPS to KML mapper")

	parser.add_argument('-n', '--nodeID', help = 'ID of the node to analyze', required = True, type = int)
	parser.add_argument('-s', '--startTime', help = 'Starting timestamp', required = True, type = int)
	parser.add_argument('-e', '--endTime', help = 'Ending timestamp (+24 hours by default)', required = False, type = int, default = 0)
	parser.add_argument('-o', '--operatorName', help = 'Name of the operator to filter (beware of issues when not filtering!). E.g., "voda ES"', required = False, type = str)
	parser.add_argument('-i', '--minGPSInterval', help = 'Minimum interval between GPS positions with the same modem data', required = False, type = int, default = 0)

	args = parser.parse_args()

	# Validate args
	if (args.endTime < args.startTime):
		args.endTime = args.startTime + 3600*24

	# Print parameters
	print "Modem status - GPS to KML mapper runs with the following parameters:"
	print "NodeID: {}".format(args.nodeID)
	print "StartTime: {}".format(args.startTime)
	print "EndTime: {}".format(args.endTime)
	print "OperatorName: {}".format(args.operatorName)
	print "MinGPSInterval: {}".format(args.minGPSInterval)

	return args

###############################################################################
if __name__ == '__main__':
	print "THE ANALYSIS COVERS FROM THE MINIMUM COMMON TIMESTAMP TO THE MAXIMUM COMMON TIMESTAMP\n\n"
	args = ParseCommandLine()

	# Connect to the DB
	auth = PlainTextAuthProvider(username = "monroe", password = "MMBNinE")
	cluster = Cluster(contact_points = ['127.0.0.1'], port = 9042, auth_provider = auth)
	session = None
	session = cluster.connect("monroe") # Set default keyspace to 'monroe'
	session.default_timeout = None
	session.default_fetch_size = 1000

	iccids = FetchNodeICCIDs(session, args.nodeID)
	print "Node {} has ICCIDs: {}\n".format(args.nodeID, iccids)
        gps = FetchPositions(session, args.startTime, args.endTime, args.nodeID);
	modem = FetchModemStatus(session, args.startTime, args.endTime, args.nodeID, iccids, args.operatorName);
	combinedEntries = TraverseGPSAndModem(gps, modem, args.minGPSInterval)
	DumpKML(args.nodeID, args.startTime, args.endTime, combinedEntries)

	#print "Total combined entries: {}".format(len(combinedEntries))
	#for entry in combinedEntries:
	#	print "{}, {}, {}, {}".format(
	#		entry['description'], entry['longitude'], entry['latitude'], entry['altitude'])
	#	print "------------\n"
	#print "------ GPS ------"
	#for ii in gps:
	#	print "{}, {}, {}, {}".format(ii.timestamp, ii.latitude, ii.longitude, ii.altitude)
	#print "------ MODEM ------"
	#for ii in modem:
	#	print "{}, {}, {}, {}, {}, {}".format(ii.timestamp, ii.iccid, ii.devicestate, ii.devicemode, ii.devicesubmode, ii.interfacename)

	cluster.shutdown() # Closes connection to the DB and frees resources.

	print "DUMP FINISHED.\n"

