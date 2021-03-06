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
from datetime import date, datetime
import os
import sys
from glob import iglob
import argparse
import textwrap
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import fnmatch
import monroevalidator
import lzma
import errno
import syslog

from cassandra.cluster import Cluster
# from cassandra.query import Statement
from cassandra.query import dict_factory
from cassandra import ConsistencyLevel
from cassandra import InvalidRequest
from cassandra.auth import PlainTextAuthProvider

CMD_NAME = os.path.basename(__file__)
DEBUG = False
VERBOSITY = 1


def log_msg(log_str, syslog_level, verbosity_level):
    """Handles syslog and console messages."""
    if not DEBUG:
        syslog.syslog(syslog_level, log_str)
    if VERBOSITY > verbosity_level:
        print (log_str)


def parse_json(f, filename):
    """
    Parse JSON objects from open file f.

    Several objects may be present in the file, and an object may be spread
    across several lines. Two objects may not occupy the same line.
    """
    jsons = []
    fname, fextension = os.path.splitext(filename)
    for line in f:
        # This while loops allow JSON objects to be pretty printed in the files
        # WARNING: A single corrupt JSON object invalidates the entire file.
        # RATIONALE: To ease debug/eror tracking
        # (ie do not modify original faulty file)
        # FIXME: If it becomes a performance problem

        # Skip empty lines, ie only whitespace
        if not line.strip():
            continue

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

    #TODO : Check why xz is on multiple line
    if (not each_json_on_single_line):
        log_str = ("possible performance hit : file {} contains "
                   "pretty printed JSON objects").format(filename)
        log_msg(log_str, syslog.LOG_WARNING, 1)
    return jsons


def construct_filepath(filename, dest_dir, middlefix="", extension=None):
    fname, fextension = os.path.splitext(filename)
    if (extension is None):
        extension = fextension
    dest_name = "{}{}{}".format(fname, middlefix, extension)
    dest_name = dest_name.strip()
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
        fname, fextension = os.path.splitext(filename)
        if fextension.endswith('.xz'):
            # WORKAROUND to avoid CRASH in LZMAFile 
	    with open(filename, 'rb') as f:
		complete_file =  iter(lzma.LZMADecompressor().decompress(f.read()).splitlines())
                json_store.extend(parse_json(complete_file, filename))
        elif fextension.endswith('.json'):
            with open(filename, 'r') as f:
                json_store.extend(parse_json(f, filename))
        else:
            raise Exception("Unknown fileformat {}".format(fextension))

        nr_jsons = len(json_store)
        dest_path = filename + ".wip"
        if not DEBUG:
            os.rename(filename, dest_path)

        filename = dest_path
    # Fail: We could not parse the file
    except Exception as error:
        dest_path = construct_filepath(filename, failed_dir, "_parse-error")
        log_str = "{} in file, moving {} to {}".format(error,
                                                       filename,
                                                       dest_path)
        log_msg(log_str, syslog.LOG_ERR, 1)
        if not DEBUG:
            os.rename(filename, dest_path)

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
                (data_ok, log_str) = monroevalidator.check(j, VERBOSITY)
                if not data_ok:
                    raise Exception("Validation error : {}".format(log_str))
                session.execute(prepared_statements[data_id],
                                [json.dumps(j)])
            processed_inserts.append(nr)

        except Exception as error:
            failed_inserts.append((nr, str(error)))

    # If all is ok move file as-is to processed (low-cost)
    if len(failed_inserts) == 0:
        dest_path = construct_filepath(filename,
                                       processed_dir,
                                       "",
                                       "")
        log_str = ("Succeded {} insert(s) (all) from file {} "
                   "moving to {}").format(nr_jsons,
                                          filename,
                                          dest_path)
        log_msg(log_str, syslog.LOG_INFO, 1)
        if not DEBUG:
            os.rename(filename, dest_path)

    # IF all is bad move file as-is to failed (low-cost)
    elif len(failed_inserts) == nr_jsons:
        dest_path = construct_filepath(filename,
                                       failed_dir,
                                       "",
                                       "")

        log_str = ("Failed {} (all) insert(s) in file {} "
                   "moving to {}; ").format(nr_jsons,
                                            filename,
                                            dest_path)
        for nr, error in failed_inserts:
            log_str += "{} Failed with {}, ".format(nr, error)

        log_msg(log_str, syslog.LOG_ERR, 1)
        if not DEBUG:
            os.rename(filename, dest_path)

    # If some fail and some succed write the ones that failed to failed dir
    # and rest to processed dir (high-cost)
    else:
        dest_path_failed = construct_filepath(filename,
                                              failed_dir,
                                              "_failed-part",
                                              ".json")

        dest_path_processed = construct_filepath(filename,
                                                 processed_dir,
                                                 "_processed-part",
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
        log_msg(log_str_error, syslog.LOG_ERR, 1)
        log_msg(log_str_processed, syslog.LOG_INFO, 1)
        if not DEBUG:
            os.unlink(filename)

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
                     prepared_statements,
                     recursive):
    """Traverse the directory tree and kick off workers to handle the files."""
    file_count = 0
    pool = ThreadPool(processes=concurrency)
    async_results = []

    # Create outdirs
    dest_dir_processed = processed_dir + str(date.today())
    try:
        os.makedirs(dest_dir_processed)
    except OSError as e:
        # If the directory already exist do nothing
        if e.errno != errno.EEXIST:
            raise e

    dest_dir_failed = failed_dir + str(date.today())
    try:
        os.makedirs(dest_dir_failed)
    except OSError as e:
        # If the directory already exist do nothing
        if e.errno != errno.EEXIST:
            raise e

    # Scan in_dir and look for all files ending in .json excluding
    # processsed_dir and failed_dir to avoid insert "loops"
    for root, dirs, files in os.walk(in_dir, topdown=True):
        if not recursive and len(dirs) > 0:
            dirs[:] = dirs[0]
        for extension in ('*.json', '*.xz'):
            for filename in fnmatch.filter(files, extension):
                path = os.path.join(root, filename)
                file_count += 1
		log_msg("Start : {}".format(path), syslog.LOG_INFO, 1)
                result = pool.apply_async(handle_file,
                                          (path,
                                           dest_dir_failed,
                                           dest_dir_processed,
                                           session,
                                           prepared_statements,))
                async_results.append(result)

    pool.close()
    pool.join()

    results = None
    try:
        results = [async_result.get() for async_result in async_results]
        # Parse errors generate inserts = -1, failed = 0
    except Exception as error:
        log_str = "Error in reading return values {}".format(error)
        log_msg(log_str, syslog.LOG_ERR, 0)

    if results is None:
        insert_count = 0
        failed_count = 0
        failed_parse_files_count = 0
        failed_insert_files_count = 0
    else:
        try:
            insert_count = sum([e['inserts'] for e in results if e['inserts'] > 0])
            failed_count = sum([e['failed'] for e in results])
            failed_parse_files_count = len([e for e in results if e['inserts'] < 0])
            failed_insert_files_count = len([e for e in results
                                            if (e['inserts'] >= 0 and
                                                e['inserts'] < e['failed'])])
        except Exception as error:
            log_str = "Error in reading return values {}:".format(error)
            log_str += ",".join(results)
            log_msg(log_str, syslog.LOG_ERR, 0)

            insert_count = 0
            failed_count = 0
            failed_parse_files_count = 0
            failed_insert_files_count = 0

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
                shutoff_time,
                in_dir,
                failed_dir,
                processed_dir,
                concurrency,
                prepared_statements,
                recursive):
    """Scan in_dir for files."""
    while True:
        start_time = time.time()
        log_str = "Start parsing files."
        log_msg(log_str, syslog.LOG_INFO, 0)
        (files,
         inserts,
         failed_inserts,
         parse_error_files,
         insert_error_files) = schedule_workers(in_dir,
                                                failed_dir,
                                                processed_dir,
                                                concurrency,
                                                session,
                                                prepared_statements,
                                                recursive)

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
        log_msg(log_str, syslog.LOG_INFO, 0)

        # If we have a "timer" set return if it is due
        if (shutoff_time > 0 and time.time() > shutoff_time):
            diff = shutoff_time - time.time()
            log_str = "Exiting due to shutoff timer: {}".format(diff)
            log_msg(log_str, syslog.LOG_INFO, 0)
            break

        # Wait if interval > 0 else return
        if (interval > 0):
            wait = interval - elapsed if (interval - elapsed > 0) else 0
            log_str = "Now waiting {} s before next run".format(wait)
            log_msg(log_str, syslog.LOG_INFO, 0)
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
    parser.add_argument('-i', '--interval',
                        metavar='N',
                        type=int,
                        default=-1,
                        help="Seconds between scans (default -1, run once)")
    parser.add_argument('-s', '--shutoff',
                        metavar='N',
                        type=int,
                        default=-1,
                        help="How many hours to run (default -1, run forever)")
    parser.add_argument('-c', '--concurrency',
                        metavar='N',
                        default=1,
                        type=int,
                        choices=range(1, max_concurrency),
                        help=("number of cores to utilize ("
                              "default 1, "
                              "max={})").format(max_concurrency - 1))
    parser.add_argument('--verbosity',
                        default=1,
                        type=int,
                        choices=range(0, 3),
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
    parser.add_argument('-r', '--recursive',
                        action="store_true",
                        help="recurse into subdirectries")
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

    # If we have a end time set create a shutoff time (epoch)
    if (args.shutoff > 0):
        shutoff_time = time.time() + 60*60*args.shutoff
    else:
        shutoff_time = -1

    return (db_user, db_password, failed_dir, processed_dir, shutoff_time)

if __name__ == '__main__':
    parser = create_arg_parser()
    args = parser.parse_args()

    (db_user,
     db_password,
     failed_dir,
     processed_dir,
     shutoff_time) = parse_special_args( args, parser)
    DEBUG = args.debug
    VERBOSITY = args.verbosity

    if (failed_dir.startswith(os.path.realpath(args.indir)+'/') or
            processed_dir.startswith(os.path.realpath(args.indir)+'/')):
        log_str = ("--failed ({}) or --processed ({}) "
                   "is a subpath of --indir ({})"
                   ", exiting").format(failed_dir, processed_dir, args.indir)
        log_msg(log_str, syslog.LOG_ERR, 0)
        raise SystemExit(1)

    # Assuming default port: 9042, clusters and sessions are longlived and
    # should be reused
    session = None
    cluster = None
    prepared_statements = {}
    if not DEBUG:
        auth = PlainTextAuthProvider(username=db_user, password=db_password)
        cluster = Cluster(args.hosts, auth_provider=auth, protocol_version=4)
        session = cluster.connect(args.keyspace)
        session.row_factory = dict_factory
        table_names = list(cluster.metadata.keyspaces[args.keyspace].tables.keys())
        for table_name in table_names:
            query = 'INSERT INTO {} JSON ?'.format(table_name)
            data_id = table_name.replace('_', '.')
            prepared_statements[data_id] = session.prepare(query)
    else:
        date_shutoff = (datetime.
                        fromtimestamp(shutoff_time).
                        strftime('%Y-%m-%d %H:%M:%S'))
        print(("Debug mode: will not insert any posts or move any files\n"
              "Info and Statements are printed to stdout\n"
              "{} called with variables \nuser={} \npassword={} \nhost={} "
              "\nkeyspace={} \nindir={} \nfaileddir={} \nprocessedir={} "
              "\nrecursive={} "
              "\ninterval={} "
              "\nConcurrency={} "
              "\nshutoff_time={}").format(CMD_NAME,
                                           db_user,
                                           db_password,
                                           args.hosts,
                                           args.keyspace,
                                           args.indir,
                                           failed_dir,
                                           processed_dir,
                                           args.recursive,
                                           args.interval,
                                           args.concurrency,
                                           date_shutoff))

    parse_files(session,
                args.interval,
                shutoff_time,
                args.indir,
                failed_dir,
                processed_dir,
                args.concurrency,
                prepared_statements,
                args.recursive)

    if not DEBUG:
        cluster.shutdown()
