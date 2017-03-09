#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Jonas Karlsson
# Date: May 2016
# License: GNU General Public License v3
# Developed for use by the EU H2020 MONROE project

"""
Used by monore_dbimporter to validate entries before importing into database.

The entry must be a python dictionary. The structure is that each dataid/table
have a seperate function that does the check and returns
True/False if the checks pass/fail.
If no check exist for a given DataId it will silently accept it
(it may fail later at db impoort though).

The module should not try to duplicate functionality found in the Cassandra db.
Ie unless absolutley necessary this module should not check type or existance
of a Db "primary keys" or for extra keys not in the db
(as this will be implicitly checked at db import).
It is ok to check for keys that are not enforced by the db if so desired but
it is the dbs responsibility to ensure that necessary keys exist in the table
and that the table exist).
"""
from datetime import datetime, timedelta
import syslog
from monroe_dbimporter import log_msg

# User defined checks should not be called directly
# Return value: True or "Error message"

TS_GRACE = timedelta(weeks=2)  # set to False to disable ts sanity checks


def _ts_sanity_check(ts):
        if TS_GRACE and ts is not None:
            ts_date = datetime.fromtimestamp(ts)
            now = datetime.now()
            return TS_GRACE > now - ts_date
        else:
            return True


def _default_accept(entry, VERBOSITY):
    log_str = ("No validity test for DataId : {} "
               "-> silently pass").format(entry.get('DataId'))
    log_msg(log_str, syslog.LOG_INFO, 1)
    return True


def _check_ping(entry, VERBOSITY):
    """
    Do some simple checks on the ping container so the values are reasonable.
    """
    try:
        if 'Rtt' in entry and 'Bytes' in entry:
            return (entry['SequenceNumber'] >= 0 and
                    entry['Rtt'] > 0 and
                    entry['Bytes'] > 0 and
                    'Timestamp' in entry) or "Value error."
        else:
            return (entry['SequenceNumber'] >= 0 and
                    'Timestamp' in entry) or "Value error."
    except Exception as error:
        return "Missing value in entry {}".format(error)

checks = {
  'MONROE.EXP.PING': _check_ping,
}


def check(entry, VERBOSITY):
    """
    Validate so the keys/values are reasonable.
    Returns (True/False, True/"Error message")
    """
    if not _ts_sanity_check(entry.get('Timestamp')):
        result = ("Input validation failed:"
                " Timestamp is older than {}").format(TS_GRACE)
        return (False, result)

    dataid = entry.get('DataId')
    if dataid is not None:
        result = checks.get(dataid, _default_accept)(entry, VERBOSITY)
    else:
        result = "Input validation failed due to missing DataId"

    if result is not True:
        log_msg(result, syslog.LOG_WARNING, 1)
    return (result is True, result)
