#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Jonas Karlsson
# Date: April 2016
# License: GNU General Public License v3
# Developed for use by the EU H2020 MONROE project

r"""
Very simple data importer that inserts json objects into a Cassandra db.

All files in, the specified directory and all subdirectories excluding
failed and done directories, containing
json objects are read and parsed into CQL (Cassandra) statements.

Note: A json object must end with a newline \n (and should for performance
 be on a single line).
 The program is (read tries to be) designed after http://tinyurl.com/q82wtpc
"""
import json
import time
import datetime
import os
import sys
from glob import iglob
import argparse
import textwrap
import syslog
from multiprocessing.pool import ThreadPool, cpu_count
import fnmatch

from cassandra.cluster import Cluster
# from cassandra.query import Statement
from cassandra.query import dict_factory
from cassandra import ConsistencyLevel
from cassandra import InvalidRequest
from cassandra.auth import PlainTextAuthProvider

CMD_NAME = os.path.basename(__file__)
DEBUG = False
VERBOSITY = 1


def parse_json(f):
    """
    Parse JSON objects from open file f.

    Several objects may be present in the file, and an object may be spread
    across several lines. Two objects may not occupy the same line.
    """
    jsons = []
    for line in f:
        # This while loops allow JSON objects to be pretty printed in the files
        # WARNING: A single corrupt JSON object invalidates the entire file.
        # RATIONALE: To ease debug/eror tracking
        # (ie do not modify original faulty file)
        # FIXME: If it becomes a performance problem

        each_json_on_single_line = True
        while True:
            # Try to build JSON (will fail if the object is not complete)
            # One could use a {} pattern matching algorithm for avoiding
            # try/catch however this is "hairy" as JSON allows {} inside
            # strings, see comment by Petr Viktorin http://tinyurl.com/gvwq7cy
            try:
                jsons.append(json.loads(line))
                break
            except ValueError:
                # Not yet a complete JSON value add next line and try again
                try:
                    line += next(f)
                    each_json_on_single_line = False
                except StopIteration as error:
                    # End of file without complete JSON object; probably a
                    # malformed file, discard entire file for now
                    raise Exception("Parse Error {}".format(error))

    if (not each_json_on_single_line):
        syslog.syslog(syslog.LOG_WARNING,
                      ("possible performance hit : file {} contains "
                       "pretty printed JSON objects").format(f.name))
    return jsons


def construct_filepath(filename, dest_dir, middlefix="", extension=None):
    fname, fextension = os.path.splitext(filename)
    if (extension is None):
        extension = fextension
    dest_name = "{}{}{}".format(fname, middlefix, extension)
    return os.path.join(dest_dir, os.path.basename(dest_name))


def handle_file(filename,
                failed_dir,
                processed_dir,
                session,
                prepared_statements):
    """
    Parse and insert file in db.

    Parse the file and tries to insert it into the database.
    move finished files to failed_dir and sucsseful to processed_dir.
    """
    json_statements = []
    nr_jsons = 0
    try:
        # Sanity Check 1: Zero files size and existance check
        if os.stat(filename).st_size == 0:
            raise Exception("Zero file size")

        json_store = []
        # Read and parse file
        with open(filename, 'r') as f:
            json_store.extend(parse_json(f))

        nr_jsons = len(json_store)
        fname, fextension = os.path.splitext(filename)
        dest_path = fname + ".wip"
        if not DEBUG:
            os.rename(filename, dest_path)

        filename = dest_path
    # Fail: We could not parse the file
    except Exception as error:
        dest_path = construct_filepath(filename, failed_dir, "_parse-error")
        log_str = "{} in file, moving {} to {}".format(error,
                                                       filename,
                                                       dest_path)
        if VERBOSITY > 1:
            print log_str
        if not DEBUG:
            os.rename(filename, dest_path)
            syslog.syslog(syslog.LOG_ERR, log_str)

        return {'inserts': -1, 'failed': 0}

    # Try to insert queries into db
    # This code assuems there is no breakage during the import
    # (ie the importer is not stopped while trying to do inserts)
    # If so happens there will be a .wip file left in the indir
    # and we are left in incosisten state that needs manual handling
    failed_inserts = []
    processed_inserts = []
    for nr, j in enumerate(json_store):
        try:
            if not DEBUG:
                data_id = j['DataId'].lower()
                session.execute(prepared_statements[data_id],
                                [json.dumps(j)])
            processed_inserts.append(nr)

        except Exception as error:
            failed_inserts.append((nr, error))

    # If all is ok move file as-is to processed (low-cost)
    if len(failed_inserts) == 0:
        dest_path = construct_filepath(filename,
                                       processed_dir,
                                       "",
                                       ".json")
        log_str = ("Succeded {} insert(s) (all) from file {} "
                   "moving to {}").format(nr_jsons,
                                          filename,
                                          dest_path)
        if VERBOSITY > 1:
            print log_str
        if not DEBUG:
            os.rename(filename, dest_path)
            syslog.syslog(syslog.LOG_INFO, log_str)

    # IF all is bad move file as-is to failed (low-cost)
    elif len(failed_inserts) == nr_jsons:
        dest_path = construct_filepath(filename,
                                       failed_dir,
                                       "",
                                       ".json")

        log_str = ("Failed {} (all) insert(s) in file {} "
                   "moving to {}; ").format(nr_jsons,
                                            filename,
                                            dest_path)
        for nr, error in failed_inserts:
            log_str += "{} Failed with {}, ".format(nr, error)

        if VERBOSITY > 1:
            print log_str
        if not DEBUG:
            os.rename(filename, dest_path)
            syslog.syslog(syslog.LOG_ERR, log_str)

    # If some fail and some succed write the ones that failed to failed dir
    # and rest to processed dir (high-cost)
    else:
        # 0 = nr failed, 1 = error message
        nr_failed = [e[0] for e in failed_inserts]
        middlefix_failed = "_{}".format("-".join(nr_failed))
        dest_path_failed = construct_filepath(filename,
                                              failed_dir,
                                              middlefix_failed,
                                              ".json")

        middlefix_processed = "_{}".format("-".join(processed_inserts))
        dest_path_processed = construct_filepath(filename,
                                                 processed_dir,
                                                 middlefix_processed,
                                                 ".json")
        log_str_error = ("Failed {} ({}) inserts in file {} "
                         "saving in {};").format(len(failed_inserts),
                                                 nr_jsons,
                                                 filename,
                                                 dest_path_failed)
        for nr, error in failed_inserts:
            log_str_error += "{} Failed with {}, ".format(nr, error)

        log_str_processed = ("Succeded with {} ({}) insert(s) in file {} "
                             "saving in {}").format(len(processed_inserts),
                                                    nr_jsons,
                                                    filename,
                                                    dest_path_processed)
        if VERBOSITY > 1:
            print log_str_processed
            print log_str_error
        if not DEBUG:
            os.unlink(filename)
            syslog.syslog(syslog.LOG_ERR, log_str_error)
            syslog.syslog(syslog.LOG_INFO, log_str_processed)

            with open(dest_path_failed, 'w') as f:
                for nr, error in failed_inserts:
                    f.write(json.dumps(json_store[nr]))
                    f.write(os.linesep)

            with open(dest_path_processed, 'w') as f:
                for nr in processed_inserts:
                    f.write(json.dumps(json_store[nr]))
                    f.write(os.linesep)

    return {'inserts': len(processed_inserts), 'failed': len(failed_inserts)}


def schedule_workers(in_dir,
                     failed_dir,
                     processed_dir,
                     concurrency,
                     session,
                     prepared_statements):
    """Traverse the directory tree and kick off workers to handle the files."""
    file_count = 0
    pool = ThreadPool(processes=concurrency)
    async_results = []

    # Create outdirs
    dest_dir_processed = processed_dir + str(datetime.date.today())
    try:
        os.stat(dest_dir_processed)
    except:
        os.makedirs(dest_dir_processed)

    dest_dir_failed = failed_dir + str(datetime.date.today())
    try:
        os.stat(dest_dir_failed)
    except:
        os.makedirs(dest_dir_failed)

    # Scan in_dir and look for all files ending in .json excluding
    # processsed_dir and failed_dir to avoid insert "loops"
    for root, dirs, files in os.walk(in_dir):
        for filename in fnmatch.filter(files, '*.json'):
            path = os.path.join(root, filename)
            file_count += 1
            result = pool.apply_async(handle_file,
                                      (path,
                                       dest_dir_failed,
                                       dest_dir_processed,
                                       session,
                                       prepared_statements,))
            async_results.append(result)

    pool.close()
    pool.join()
    results = [async_result.get() for async_result in async_results]
    # Parse errors generate inserts = -1, failed = 0
    insert_count = sum([e['inserts'] for e in results if e['inserts'] > 0])
    failed_count = sum([e['failed'] for e in results])
    failed_parse_files_count = len([e for e in results if e['inserts'] < 0])
    failed_insert_files_count = len([e for e in results
                                     if (e['inserts'] >= 0 and
                                         e['inserts'] < e['failed'])])

    # Remove empty dirs
    try:
        # Will only succed if the directory is empty
        os.rmdir(dest_dir_failed)
    except OSError as e:
        # If the directory is not empty we do nothing
        pass
    try:
        # Will only succed if the directory is empty
        os.rmdir(dest_dir_processed)
    except OSError as e:
        # If the directory is not empty we do nothing
        pass

    return (file_count,
            insert_count,
            failed_count,
            failed_parse_files_count,
            failed_insert_files_count)


def parse_files(session,
                interval,
                in_dir,
                failed_dir,
                processed_dir,
                concurrency,
                prepared_statements):
    """Scan in_dir for files."""
    while True:
        start_time = time.time()
        if VERBOSITY > 0:
            print('Start parsing files.')
        (files,
         inserts,
         failed_inserts,
         parse_error_files,
         insert_error_files) = schedule_workers(in_dir,
                                                failed_dir,
                                                processed_dir,
                                                concurrency,
                                                session,
                                                prepared_statements)

        # Calculate time we should wait to satisfy the interval requirement
        elapsed = time.time() - start_time
        log_str = ("Parsing {} files and doing "
                   "{} inserts took {} s; "
                   "{} inserts").format(files,
                                        inserts,
                                        elapsed,
                                        failed_inserts)
        if parse_error_files + insert_error_files > 0:
            log_str += (" and {} files (parse error: {},"
                        " insert error (full or partly): {})"
                        "").format(insert_error_files + parse_error_files,
                                   parse_error_files,
                                   insert_error_files)
        log_str += " failed"
        if not DEBUG:
            syslog.syslog(log_str)
        if VERBOSITY > 0:
            print log_str
        # Wait if interval > 0 else finish loop and return
        if (interval > 0):
            wait = interval - elapsed if (interval - elapsed > 0) else 0
            log_str = "Now waiting {} s before next run".format(wait)
            if not DEBUG:
                syslog.syslog(log_str)
            if VERBOSITY > 0:
                print log_str
            time.sleep(wait)
        else:
            break


def create_arg_parser():
    """Create a argument parser and return it."""
    max_concurrency = cpu_count()
    parser = argparse.ArgumentParser(
        prog=CMD_NAME,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''
            Parses .json files in in_dir and inserts them into Cassandra
            Cluster specified in -H/--hosts.
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
    parser.add_argument('-i', '--interval',
                        metavar='N',
                        type=int,
                        default=-1,
                        help="Seconds between scans (default -1, run once)")
    parser.add_argument('-c', '--concurrency',
                        metavar='N',
                        default=1,
                        type=int,
                        choices=xrange(1, max_concurrency),
                        help=("number of cores to utilize ("
                              "default 1, "
                              "max={})").format(max_concurrency - 1))
    parser.add_argument('--verbosity',
                        default=1,
                        type=int,
                        choices=xrange(0, 3),
                        help="Verbosity level 0-2(default 1)")
    parser.add_argument('-I', '--indir',
                        metavar='DIR',
                        default="/experiments/monroe",
                        help=("Directory to scan (default "
                              "/experiments/monroe/)"))
    parser.add_argument('-F', '--failed',
                        metavar='DIR',
                        default="/experiments/failed",
                        help=("Failed files (default "
                              "/experiments/failed(-$DATE))"))
    parser.add_argument('-P', '--processed',
                        metavar='DIR',
                        default="/experiments/processed",
                        help=("Processed files files (default "
                              "/experiments/processed(-$DATE))"))
    parser.add_argument('--debug',
                        action="store_true",
                        help="Do not execute queries or move files")
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
    failed_dir = None
    processed_dir = None
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

    # Default values of failed and processed dirs i dependent on args.indir
    failed_dir = os.path.realpath(args.failed) + os.sep
    processed_dir = os.path.realpath(args.processed) + os.sep

    return (db_user, db_password, failed_dir, processed_dir)

if __name__ == '__main__':
    parser = create_arg_parser()
    args = parser.parse_args()

    db_user, db_password, failed_dir, processed_dir = parse_special_args(
        args,
        parser)
    DEBUG = args.debug
    VERBOSITY = args.verbosity

    if (failed_dir.startswith(os.path.realpath(args.indir)+'/') or
            processed_dir.startswith(os.path.realpath(args.indir)+'/')):
        log_str = ("--failed ({}) or --processed ({}) "
                   "is a subpath of --indir ({})"
                   ", exiting").format(failed_dir, processed_dir, args.indir)
        if not DEBUG:
            syslog.syslog(syslog.LOG_ERR, log_str)
        print log_str
        raise SystemExit(1)

    # Assuming default port: 9042, clusters and sessions are longlived and
    # should be reused
    session = None
    cluster = None
    prepared_statements = {}
    if not DEBUG:
        auth = PlainTextAuthProvider(username=db_user, password=db_password)
        cluster = Cluster(args.hosts, auth_provider=auth)
        session = cluster.connect(args.keyspace)
        session.row_factory = dict_factory
        table_names = cluster.metadata.keyspaces[args.keyspace].tables.keys()
        for table_name in table_names:
            query = 'INSERT INTO {} JSON ?'.format(table_name)
            data_id = table_name.replace('_', '.')
            prepared_statements[data_id] = session.prepare(query)
    else:
        print("Debug mode: will not insert any posts or move any files\n"
              "Info and Statements are printed to stdout\n"
              "{} called with variables \nuser={} \npassword={} \nhost={} "
              "\nkeyspace={} \nindir={} \nfaileddir={} \nprocessedir={} "
              "\ninterval={} \nConcurrency={}\n").format(CMD_NAME,
                                                         db_user,
                                                         db_password,
                                                         args.hosts,
                                                         args.keyspace,
                                                         args.indir,
                                                         failed_dir,
                                                         processed_dir,
                                                         args.interval,
                                                         args.concurrency)

    parse_files(session,
                args.interval,
                args.indir,
                failed_dir,
                processed_dir,
                args.concurrency,
                prepared_statements)

    if not DEBUG:
        cluster.shutdown()
