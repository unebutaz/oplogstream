#!/usr/bin/env python
import glob
import os
import sys
import locale
import logging
import logging.handlers
import daemon
import ConfigParser

from handlers import QueueHandler, OpFilter
from watcher import OplogWatcher

CONFIG_FILTER_SECTION = 'Filter'
CONFIG_MONGO_SECTION = 'MongoDB'
CONFIG_RABBIT_SECTION = 'RabbitMQ'

DEFAULT_OUT_FILE = '/tmp/oplogstreamd.out'

# manage configuration
try:
    path = os.path.realpath(__file__)
    path = os.path.dirname(os.path.dirname(path))

    config = ConfigParser.ConfigParser()

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

try:
    logFile = config.get('Main', 'logFile')
except ConfigParser.NoOptionError:
    logFile = '/tmp/oplogstreamd.log'

try:
    pidFile = config.get('Main', 'pidFile')
except ConfigParser.NoOptionError:
    pidFile = '/tmp/oplogstreamd.pid'

# Set up logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.handlers.RotatingFileHandler(logFile)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

class Oplogstreamd(daemon.Daemon):
    def run(self):
        mongodb_options = dict(config.items(CONFIG_MONGO_SECTION))
        rabbitmq_options = dict(config.items(CONFIG_RABBIT_SECTION))
        op_handler = QueueHandler(**rabbitmq_options)

        if config.has_section(CONFIG_FILTER_SECTION) and len(config.items(CONFIG_FILTER_SECTION)):
            databses = config.get(CONFIG_FILTER_SECTION, 'databases').split(',')
            collections = config.get(CONFIG_FILTER_SECTION, 'collections').split(',')
            operations = config.get(CONFIG_FILTER_SECTION, 'operations').split(',')
            op_handler.add_filter(OpFilter(databses, collections, operations))

        oplog_watcher = OplogWatcher(op_handler, **mongodb_options)
        oplog_watcher.start()


def main():
    locale.setlocale(locale.LC_ALL)
    # check access to pid file
    # define system pipes
    # instantiate custom daemon class
    daemon = Oplogstreamd(pidfile=pidFile, stdout=DEFAULT_OUT_FILE, stderr=DEFAULT_OUT_FILE, verbose=0)
    actions = {
        'start': daemon.start,
        'stop': daemon.stop,
        'restart': daemon.restart,
        'run': daemon.run
    }

    if len(sys.argv) > 1 and sys.argv[1] in actions:
        actions[sys.argv[1]]()
    else:
        print 'Unknown command. Use `oplogstreamd %s`' % '|' . join(actions.keys())


if __name__ == '__main__':
    main()

