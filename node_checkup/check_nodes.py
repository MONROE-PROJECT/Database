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
from cassandra.query import dict_factory, ordered_dict_factory
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
    parser.add_argument('--verbose',
                        action="store_true",
                        help="Verbose output")
    parser.add_argument('-v', '--version',
                        action="version",
                        version="%(prog)s 1.0")
    return parser


def parse_special_args(args, parser, now):
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
    return (int(now - span), db_user, db_password)



if __name__ == '__main__':
    parser = create_arg_parser()
    args = parser.parse_args()
    now = time.time()
    span, db_user, db_password = parse_special_args(args, parser, now)

    # Assuming default port: 9042, clusters and sessions are longlived and
    # should be reused
    session = None
    cluster = None
    results = {}
    auth = PlainTextAuthProvider(username=db_user, password=db_password)
    cluster = Cluster(args.hosts, auth_provider=auth)
    session = cluster.connect(args.keyspace)
    session.row_factory = dict_factory
    #session.row_factory = ordered_dict_factory
    keyspace = cluster.metadata.keyspaces[args.keyspace]
    table_names = keyspace.tables.keys()
    # Check for tables not containing obligatory keys
    print "{:#<80.80}".format("#### These tables does not contain "
           "timestamp or/and nodeid ")
    for table_name in table_names:
        col_names = keyspace.tables[table_name].columns.keys()
        if 'timestamp' not in col_names or 'nodeid' not in col_names:
            print ("{}").format(table_name)
    print "{:#<80}".format("")
    simple_query = {}
    error_query = {}
    spec_query = {}
    # These queries are only used to get nodeid seen in the db
    tables = ['monroe_meta_node_sensor',
              'monroe_exp_exhaustive_paris',
              'monroe_exp_simple_traceroute',
              'monroe_meta_node_event']
    for table_name in ['monroe_meta_node_sensor',
                       'monroe_exp_exhaustive_paris',
                       'monroe_exp_simple_traceroute',
                       'monroe_meta_node_event']:
        simple_query[table_name] = ('SELECT distinct nodeid from '
                                    '{} where timestamp > {} '
                                    'ALLOW FILTERING').format(table_name, span)

    # Only check for errors
    error_query['monroe_meta_node_event'] = ('SELECT distinct nodeid '
                                             'from monroe_meta_node_event '
                                             'where timestamp > {} '
                                             'AND eventtype = '
                                             '\'Watchdog.Failed\' '
                                             'ALLOW FILTERING').format(span)

    # Specific checks
    GPS = 'monroe_meta_device_gps'
    PING = 'monroe_exp_ping'
    MODEM = 'monroe_meta_device_modem'
    HTTP = 'monroe_exp_http_download'
    spec_query[PING] = ('SELECT nodeid, '
                        'timestamp, '
                        'operator, '
                        'iccid '
                        'from {} '
                        'where timestamp > {} '
                        'ALLOW FILTERING').format(PING, span)
    spec_query[HTTP] = ('SELECT nodeid, '
                        'timestamp, '
                        'operator, '
                        'iccid '
                        'from {} '
                        'where timestamp > {} '
                        'ALLOW FILTERING').format(HTTP, span)
    spec_query[MODEM] = ('SELECT nodeid, '
                         'timestamp, '
                         'operator, '
                         'iccid, '
                    #     'internalipaddress, '
                         'internalinterface, '
                    #     'ipaddress, '
                         'interfacename '
                         'from {} '
                         'where timestamp > {} '
                         'ALLOW FILTERING').format(MODEM, span)
    spec_query[GPS] = ('SELECT nodeid, timestamp '
                       'from {} '
                       'where timestamp > {} '
                       'ALLOW FILTERING').format(GPS, span)

    # Existance checks
    nodes_seen = set()
    for table_name, query in simple_query.iteritems():
        try:
            for row in session.execute(query):
                nodes_seen.add(int(row['nodeid']))
        except Exception as e:
            print "Error for table {} : {}".format(table_name, e)

    nodes = {}
    for node in nodes_seen:
        nodes[node] = {}

    # Error check
    for table_name, query in error_query.iteritems():
        try:
            for row in session.execute(query):
                nodes[int(row['nodeid'])]['Watchdog.failed'] = True
        except Exception as e:
            print "Error for table {} : {}".format(table_name, e)

    # Specific checks
    for table_name, query in spec_query.iteritems():
        try:
            for row in session.execute(query):
                nodeid = int(row['nodeid'])
                ts = float(row['timestamp'])
                if nodeid not in nodes:
                    nodes[nodeid] = {}

                if table_name == GPS:
                    if table_name not in nodes[nodeid]:
                        nodes[nodeid][table_name] = []

                    nodes[nodeid][table_name].append(ts)

                if table_name in [PING, HTTP]:
                    iccid = str(row['iccid'])
                    op = str(row['operator'].encode('utf-8'))
                    if table_name not in nodes[nodeid]:
                        nodes[nodeid][table_name] = {}

                    if iccid not in nodes[nodeid][table_name]:
                        nodes[nodeid][table_name][iccid] = {
                                                            'op': op,
                                                            'ts': []
                                                            }

                    nodes[nodeid][table_name][iccid]['ts'].append(ts)

                if table_name in [MODEM]:
                    iccid = str(row['iccid'])
                    op = str(row['operator'].encode('utf-8'))
                    intif = str(row['internalinterface'])
                    hostif = str(row['interfacename'])
                    if table_name not in nodes[nodeid]:
                        nodes[nodeid][table_name] = {}

                    if iccid not in nodes[nodeid][table_name]:
                        nodes[nodeid][table_name][iccid] = {
                                                            'op': op,
                                                            'intif': intif,
                                                            'if': hostif,
                                                            'ts': []
                                                            }

                    nodes[nodeid][table_name][iccid]['ts'].append(ts)

        except Exception as e:
            print "Error for table {} : {}".format(table_name, e)
            print row
            continue
    # Sort the timestamps and check for biggest time diff between timestamps
    for nodeid in nodes:
        for table_name in nodes[nodeid]:
            if table_name == GPS:
                # Sort the timestamps so we can find the biggest difference
                nodes[nodeid][table_name].sort()
                ts = nodes[nodeid][table_name]
                ts.append(now)
                # Append the time now so we can check so we have recent data
                diff = [ts[i+1] - ts[i] for i in range(len(ts)-1)]
                nodes[nodeid][table_name] = max(diff)
            if table_name in [PING, MODEM, HTTP]:
                for iccid in nodes[nodeid][table_name]:
                    # Sort the timestamps so we can find the biggest difference
                    nodes[nodeid][table_name][iccid]['ts'].sort()
                    ts = nodes[nodeid][table_name][iccid]['ts']
                    # Append the time now so we can check so we have recent data
                    ts.append(now)
                    diff = [ts[i+1] - ts[i] for i in range(len(ts)-1)]
                    nodes[nodeid][table_name][iccid]['ts'] = max(diff)
    if args.verbose:
        template = "| {: <6} | {: <15} | {: <50} | {: <80} | {: <50} |"
    else:
        template = "| {: <6} | {: <3} | {: <13} | {: <15} | {: <15} |"

    # Header
    print template.format('NodeID',
                          'GPS',
                          'Ping(operator)',
                          'Modem(operator)',
                          'HTTP(operator)')
    print template.format('---',
                          '---',
                          '---',
                          '---',
                          '---')

    #print "| {:-<6} | {:-<15} | {:-<50} | {:-<80} | {:-<50} |".format("",
    #                                                                  "",
    #                                                                  "",
    #                                                                  "",
    #                                                                  "")

    GRACE = 3600
    for node in nodes:
        results = {}
        results[GPS] = {'ok' : False}
        if GPS in nodes[node]:
            if nodes[node][GPS] > GRACE:
                results[GPS]['ok'] = False

            results[GPS]['str'] = "{}".format(int(nodes[node][GPS]))

        for table_name in [PING]:
            results[table_name] = {'ok' : False}
            if table_name in nodes[node]:
                results[table_name] = {}
                operators = []
                results[table_name]['ok'] = len(nodes[node][table_name]) == 3
                for iccid in nodes[node][table_name]:
                    op = nodes[node][table_name][iccid]['op']
                    diff = nodes[node][table_name][iccid]['ts']
                    operators.append("{}({})".format(op, int(diff)))
                    if diff > GRACE:

                        results[table_name]['ok'] = False
                while len(operators) < 3:
                    operators.append("-")

                results[table_name]['str'] = "{}".format(",".join(operators))

        for table_name in [MODEM]:
            results[table_name] = {'ok' : False}
            if table_name in nodes[node]:
                results[table_name] = {}
                operators = []
                intifnr = 0
                for iccid in nodes[node][table_name]:
                    op = nodes[node][table_name][iccid]['op']
                    intif = nodes[node][table_name][iccid]['intif']
                    hostif = nodes[node][table_name][iccid]['if']
                    diff = nodes[node][table_name][iccid]['ts']
                    if intif in ['op0', 'op1', 'op2']:
                            intifnr += 1
                    operators.append("{}-{}({})".format(op, hostif, int(diff)))

                while len(operators) < 3:
                    operators.append("-")
                results[table_name]['ok'] = intifnr == 3
                results[table_name]['str'] = "{}".format(",".join(operators))

        for table_name in [HTTP]:
            results[table_name] = {'ok' : False}
            if table_name in nodes[node]:
                results[table_name] = {}
                operators = []
                results[table_name]['ok'] = len(nodes[node][table_name]) == 3
                for iccid in nodes[node][table_name]:
                    op = nodes[node][table_name][iccid]['op']
                    operators.append("{}({})".format(op, int(diff)))
                while len(operators) < 3:
                    operators.append("-")

                results[table_name]['str'] = "{}".format(",".join(operators))

        datastr = {}
        for table_name in results:
            datastr[table_name] = str(results[table_name]['ok'])
            if args.verbose:
                if 'str' in results[table_name]:
                    datastr[table_name] += "({})".format(results[table_name]['str'])
                else:
                    datastr[table_name] += "({})".format("-")

        print template.format(node,
                              datastr[GPS],
                              datastr[PING],
                              datastr[MODEM],
                              datastr[HTTP])

    cluster.shutdown()
