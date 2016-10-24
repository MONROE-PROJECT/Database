#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Jonas Karlsson
# Date: October 2016
# License: GNU General Public License v3
# Developed for use by the EU H2020 MONROE project

r"""
Will try to determine which nodes that regularly contribute to the database.

Will look in all tables and see what nodes has contributed data.
"""
import json
import time
import datetime
import os
import sys
import argparse
import textwrap
import syslog

from cassandra.cluster import Cluster
# from cassandra.query import Statement
from cassandra.query import dict_factory
from cassandra import ConsistencyLevel
from cassandra import InvalidRequest
from cassandra.auth import PlainTextAuthProvider

CMD_NAME = os.path.basename(__file__)


def create_arg_parser():
    """Create a argument parser and return it."""
    parser = argparse.ArgumentParser(
        prog=CMD_NAME,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''
            Parses .json or (.xz) files in in_dir and inserts them into the
            Cassandra Cluster specified in -H/--hosts.
            All directories not existing will be created.'''))
    parser.add_argument('-u', '--user',
                        help="Cassandra username")
    parser.add_argument('-p', '--password',
                        help="Cassandra password")
    parser.add_argument('-H', '--hosts',
                        nargs='+',
                        default=["127.0.0.1"],
                        help="Hosts in the cluster (default 127.0.0.1)")
    parser.add_argument('-k', '--keyspace',
                        required=True,
                        help="Keyspace to use")
    parser.add_argument('-t', '--timespan',
                        required=True,
                        help=("Xw Xd Xm "
                              "What timespan to look for (now-timespan)"))
    parser.add_argument('--authenv',
                        action="store_true",
                        help=("Use environment variables MONROE_DB_USER and "
                              "MONROE_DB_PASSWD as username and password"))
    parser.add_argument('-v', '--version',
                        action="version",
                        version="%(prog)s 1.0")
    return parser


def parse_special_args(args, parser):
    """Parse and varifies user,password and failed,processed dirs."""
    db_user = None
    db_password = None
    if not args.authenv and not (args.user and args.password):
        parser.error('either --authenv or -u/--user USER and -p/--password '
                     'PASSWORD needs to be defined')

    if args.authenv:
        if 'MONROE_DB_USER' not in os.environ:
            parser.error("missing user env MONROE_DB_USER")
        if 'MONROE_DB_PASSWD' not in os.environ:
            parser.error("missing user env MONROE_DB_PASSWD")
        db_user = os.environ['MONROE_DB_USER']
        db_password = os.environ['MONROE_DB_PASSWD']

    # Specified user and password takes precedence over environment variables
    if args.user:
        db_user = args.user
    if args.password:
        db_password = args.password

    if "w" in args.timespan:
        weeks = int(args.timespan.strip('w'))
        span = weeks*3600*24*7
    elif "d" in args.timespan:
        days = int(args.timespan.strip('d'))
        span = days*3600*24
    elif "h" in args.timespan:
        hours = int(args.timespan.strip('h'))
        span = hours*3600
    elif "m" in args.timespan:
        minutes = int(args.timespan.strip('m'))
        span = hours*60
    else:
        span = int(args.timespan)
    return (int(time.time() - span), db_user, db_password)

if __name__ == '__main__':
    parser = create_arg_parser()
    args = parser.parse_args()

    span, db_user, db_password = parse_special_args(args, parser)

    # Assuming default port: 9042, clusters and sessions are longlived and
    # should be reused
    session = None
    cluster = None
    results = {}
    auth = PlainTextAuthProvider(username=db_user, password=db_password)
    cluster = Cluster(args.hosts, auth_provider=auth)
    session = cluster.connect(args.keyspace)
    session.row_factory = dict_factory
    table_names = cluster.metadata.keyspaces[args.keyspace].tables.keys()
    for table_name in table_names:
        if ('timestamp' not in cluster.metadata.keyspaces[args.keyspace].tables[table_name].columns.keys() or
            'nodeid' not in cluster.metadata.keyspaces[args.keyspace].tables[table_name].columns.keys()):
            print "Does not contain timestamp or and nodeid  {}".format(table_name)
            continue
        query = 'SELECT nodeid, timestamp from {} where timestamp > {} ALLOW FILTERING'.format(table_name, span)
        try:
            results[table_name] = session.execute(query)
        except Exception as e:
            print "Error for table {} : {}".format(table_name, e)

    for table_name, result in results.iteritems():
        nodes = set()
        try:
            for row in result:
                nodes.add(int(row['nodeid']))
        except Exception as e:
            print "Error for table {} : {}".format(table_name, e)
        print "Nodes in table {} : {}".format(table_name, ",".join([str(e) for e in nodes]))

    cluster.shutdown()
