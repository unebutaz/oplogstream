#!/usr/bin/env python

import glob
import os
import sys
import logging
import logging.handlers
import watcher
import ConfigParser
import daemon
import locale
from handlers import PrintOpHandler


defaultConfig = {
    'pid_file': '/var/run/oplogstream.pid',
    'log_file': '/tmp/oplogstream.log',
    'log_level': 'DEBUG',
    'mongodb': {
        'host': 'localhost',
        'post': '27017',
        'db': 'local',
        'colelction': 'oplog.rs',
    },
    'rabbitmq': {
        'host': 'localhost',
        'port': '5672',
        'vhost': '/',
        'exchange': ''
    }
}


# manage configuration
try:

    path = os.path.realpath(__file__)
    path = os.path.dirname(os.path.dirname(path))

    config = ConfigParser.ConfigParser(defaultConfig)

    if os.path.exists('/etc/oplogstream/conf.d/'):
        configPath = '/etc/oplogstream/conf.d/'
    elif os.path.exists('/etc/oplogstream/config.cfg'):
        configPath = '/etc/oplogstream/config.cfg'
    else:
        configPath = path + '/config.cfg'

    if not os.access(configPath, os.R_OK):
        print('Unable to read configuration file at %s' % configPath)
        sys.exit(1)

    if os.path.isdir(configPath):
        for conf in glob.glob(os.path.join(configPath, ".cfg")):
            config.read(conf)
    else:
        config.read(configPath)

except ConfigParser.Error:
    print("Configuration file not found or corrupted.")
    sys.exit(1)


# Set up logging
logFile = config.get('Main', 'log_file')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(logFile)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

class Oplogstreamd(daemon.Daemon):
    def run(self):
        handler = PrintOpHandler()
        # start oplog monitoring, incoming ops processed by handler
        # instantiate filters if needed
        watcher.OplogWatcher(handler).start()


if __name__ == '__main__':

    #set locale
    locale.setlocale(locale.LC_ALL, 'C')
    os.putenv('LC_ALL', 'C')

    # define pidfile
    pidFile = config.get('Main', 'pid_file')
    if not os.access(pidFile, os.W_OK):
        pidFile = '/tmp/oplogstream.pid'
        # print('Could not write pid file.')
        # sys.exit(1)

    # define system pipes

    # instantiate custom daemon class
    oplogstreamd = Oplogstreamd(pidfile=pidFile, stdout=logFile, stderr=logFile)
    # figure out operation
    # do action

    actionDict = {
        'start': oplogstreamd.start,
        'stop': oplogstreamd.stop,
        'restart': oplogstreamd.restart,
        'run': oplogstreamd.run,
    }

    print(sys.argv)
    if len(sys.argv) > 1 and sys.argv[1] in actionDict:
        actionDict[sys.argv[1]]()
    else:
        print 'Unknown command. Use `oplogstreamd %s`' % '|'.join(actionDict.keys())