#!/usr/bin/python

"""
 Example tool to convert GPS positions of MONROE nodes in the Cassandra database to KML.
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

def DumpPositions(session, startTime, endTime, nodeID):
	print "\n======================================================================"
	print "======================================================================"
	print "======================================================================"
	print "Extracting GPS positions for node {} during interval [{}, {})\n".format(nodeID, startTime, endTime)
	
	########## monroe_meta_device_gps #################
	session.default_fetch_size = 1000
	fileName = "{}_{}_{}.kml".format(nodeID, startTime, endTime)
	with open(fileName, "wt") as output:
                # Write KML file headers.
                output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?><kml xmlns=\"http://www.opengis.net/kml/2.2\">\n"
                                "<Document>\n"
                                #"<Style id=\"iconoPosicion\">\n"
				#"	<IconStyle>\n"
				#"    <scale>0.5</scale>\n"
				#"    <Icon>\n"
				#"      <href>MarcaRoja.png</href>\n"
				#"    </Icon>\n"
				#"  </IconStyle>\n"
				#"</Style>\n"
				"<Folder>\n")

		query = "select nmea, nodeid, timestamp, latitude, longitude, altitude, speed, satellitecount from monroe_meta_device_gps where nodeid='{}' and timestamp >= {} and timestamp < {} order by timestamp".format(nodeID, startTime, endTime)
		print query
		rows = session.execute(query, timeout=None)
		count = 0
		for row in rows:
			try:
				if row.nmea.find("GPRMC") != -1:
					description = "Latitud: {} {}\nLongitud: {} {}\nAltitud: {}\nVelocidad: {} Km/h\n".format(
                                        	row.latitude, 'N' if row.latitude >= 0.0 else 'S',
                                        	row.longitude, 'E' if row.longitude >= 0.0 else 'W',
                                        	row.altitude, row.speed)
					output.write("\n<Placemark>\n"
						"<description>{}</description>\n"
						"<styleUrl>#iconoPosicion</styleUrl>\n"
						"<Point> <coordinates>{},{},{} </coordinates> </Point>\n"
						"</Placemark>\n".format(description, row.longitude, row.latitude, row.altitude))
					count += 1
			except Exception as error:
                                print "Error in row:", row, error

                # Write KML file footers.
		output.write("</Folder>\n</Document>\n</kml>\n")
		
	print "Dumped {} positions to {}\n".format(count, fileName)


if __name__ == '__main__':

	auth = PlainTextAuthProvider(username = "xxxx", password = "yyyy")
	cluster = Cluster(contact_points = ['127.0.0.1'], port = 9042, auth_provider = auth)
	session = None
	session = cluster.connect("monroe") # Set default keyspace to 'monroe'
	session.default_timeout = None
	session.default_fetch_size = 1000

	# Thu Sep 15 2016 14:00:00 GMT+0200 (Romance Daylight Time)
	DumpPositions(session, 1473940800, 1473940800 + 3600*1, 54);

	cluster.shutdown() # Closes connection to the DB and frees resources.

	print "DUMP FINISHED.\n"

