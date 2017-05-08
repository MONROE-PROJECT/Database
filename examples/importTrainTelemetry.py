#!/usr/bin/python

"""
 Tool to import train telemetry into the GPS table.
  https://www.monroe-project.eu
  Creator: Miguel Peon Quiros, IMDEA Networks Institute
  mikepeon@imdea.org

 Dependencies: sudo pip install cassandra-driver python-dateutil

 Cassandra driver (Python) documentation: https://datastax.github.io/python-driver/index.html
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import argparse
import csv


###############################################################################
def ParseCommandLine():
	parser = argparse.ArgumentParser(description = "Train telemetry (GPS) importer")

	parser.add_argument('-n', '--nodeID', help = 'ID of the node in the train', required = True, type = int)
	parser.add_argument('-f', '--fileName', help = 'Telemetry filename (CSV)', required = True, type = str)

	args = parser.parse_args()

	# Print parameters
	print "Train telemetry (GPS) runs with the following parameters:"
	print "NodeID: {}".format(args.nodeID)
	print "StartTime: {}".format(args.fileName)

	return args

###############################################################################
if __name__ == '__main__':
	args = ParseCommandLine()

	# Connect to the DB
	auth = PlainTextAuthProvider(username = "xxxx", password = "yyyy")
	cluster = Cluster(contact_points = ['127.0.0.1'], port = 9042, auth_provider = auth)
	session = None
	session = cluster.connect("monroe") # Set default keyspace to 'monroe'
	session.default_timeout = None
	session.default_fetch_size = 1000

	with open(args.fileName, "rb") as iFile:
		theReader = csv.reader(iFile, delimiter = ',')
		count = 0
		theReader.next() # Skip CSV header
		for line in theReader:
			query = "insert into monroe_meta_device_gps (NodeId, Timestamp, DataId, DataVersion, SequenceNumber, Longitude, Latitude, Speed, SatelliteCount) values ('{}', {:.6f}, '{}', {}, {}, {}, {}, {}, {})".format(args.nodeID, int(line[2]), "MONROE.META.DEVICE.GPS", 2, count, line[6], line[5], line[8], line[7])
			#print query
			session.execute(query, timeout = None)
			count = count + 1

	print "Inserted {} lines.".format(count)

	cluster.shutdown() # Closes connection to the DB and frees resources.

	print "DUMP FINISHED.\n"

