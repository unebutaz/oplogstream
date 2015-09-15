#!/usr/bin/env python

import os
import sys
import signal
import logging
import lockfile
import daemon
import watcher
import ConfigParser

from pymongo import MongoClient
from handlers import PrintOpHandler

print sys.argv

here = os.path.dirname(os.path.dirname(sys.argv[0]))

config_path = None
paths = [
    os.path.join(here, 'oplogstream.conf'),
    os.path.join('etc', 'oplogstream.conf')
]

for p in paths:
    if os.path.exists(p):
        config_path = p
        break

try:
    config = ConfigParser.SafeConfigParser()
    if config_path is not None:
        config.read(config_path)
    else:
        raise LookupError("Could not find config file.")
except Exception as err:
    print(err.message)
    sys.exit(1)

target_dbs = config.get('filter', 'databases')
target_ops = config.get('filter', 'operations')
target_collections = config.get('filter', 'collections')

(host, port, db, collection) = config.get('mongodb', 'host'), config.get('mongodb', 'port'), \
    config.get('mongodb', 'db'), config.get('mongodb', 'collection')

connection = MongoClient(host, int(port), socketTimeoutMS=20000).get_database(db).get_collection(collection)

context = daemon.DaemonContext(
    working_directory='/',
    umask=0o002,
    pidfile=lockfile.FileLock('/home/sergey/oplogstreamd.pid'),
    )


if __name__ == '__main__':
    handler = PrintOpHandler()
    w = watcher.OplogWatcher(handler, connection)
    with context:
        w.start()