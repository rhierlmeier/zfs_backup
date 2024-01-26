#!/usr/bin/env python

"""zfs_backup.py: Create backups like rsnapshot with zfs."""

__author__      = "Richard Hierlmeier"
__copyright__   = "Copyright 2024, Richard Hierlmeier"
__license__ = "GPL-3.0"
__status__ = "Beta"
"""

import logging
import re
import sys
import os
from datetime import datetime
from typing import List

import yaml
import subprocess

DATE_TIME_PATTERN = "\\d{4}-[0-1]\\d-[0-3]\\dT[0-2]\\d:[0-6]\\d:[0-6]\\d"

DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


class Config:

    def __init__(self, pool: str, backup_pool: str, datasets: List[str], num_backups_daily: int,
                 num_backups_weekly: int,
                 num_backups_monthly: int, nums_backup_yearly: int, _log_file: str):
        self.pool = pool
        self.backup_pool = backup_pool
        self.datasets = datasets
        self.num_backups_daily = num_backups_daily
        self.num_backups_weekly = num_backups_weekly
        self.num_backups_monthly = num_backups_monthly
        self.nums_backups_yearly = nums_backup_yearly
        self.log_file = _log_file

    def get_num_backups(self, _backup_type: str):
        if _backup_type == "daily":
            return self.num_backups_daily
        if _backup_type == "weekly":
            return self.num_backups_weekly
        if _backup_type == "monthly":
            return self.num_backups_monthly
        if _backup_type == "yearly":
            return self.nums_backups_yearly

        logging.error("Unsupported backup type " + _backup_type)
        exit(1)


class Diff:
    def __init__(self, l1, l2):
        self.added = []
        self.existing = []
        self.removed = []
        for e in l1:
            if e not in l2:
                self.removed.append(e)
            else:
                self.existing.append(e)

        for e in l2:
            if e not in l1:
                self.added.append(e)


class Snapshot:
    def __init__(self, _pool, _dataset, _backup_pool, _backup_type, _backup_time):
        self.pool = _pool
        self.dataset = _dataset
        self.backup_pool = _backup_pool
        self.backup_type = _backup_type
        self.backup_time = _backup_time

    def __repr__(self):
        return self.get_full_qualified_snap_name()

    def destroy(self):
        zfs_destroy(self.get_full_qualified_snap_name())
        zfs_destroy(self.get_full_qualified_backup_snap_name())

    def get_full_qualified_backup_dataset(self):
        return self.backup_pool + "/" + self.dataset

    def get_snap_name(self):
        return self.backup_type + "-" + self.backup_time.strftime(DATE_TIME_FORMAT)

    def get_full_qualified_snap_name(self):
        return self.pool + "/" + self.dataset + "@" + self.get_snap_name()

    def get_full_qualified_backup_snap_name(self):
        return self.backup_pool + "/" + self.dataset + "@" + self.get_snap_name()

    def create(self, _last_snap=None):

        logging.info("Creating snapshot " + self.get_full_qualified_backup_snap_name())
        zfs("snapshot", self.get_full_qualified_snap_name())

        if _last_snap is None:
            _cmd = ("zfs send '{new_snap}' | zfs recv -F '{backup_snap}'"
                    .format(new_snap=self.get_full_qualified_snap_name(),
                            backup_snap=self.get_full_qualified_backup_snap_name()))
        else:
            _cmd = ("zfs send -i '{last_snap}' '{new_snap}' | zfs recv -Fu '{backup_snap}'"
                    .format(last_snap=_last_snap.get_full_qualified_snap_name(),
                            new_snap=self.get_full_qualified_snap_name(),
                            last_backup_snap=_last_snap.get_snap_name(),
                            backup_snap=self.get_full_qualified_backup_snap_name()))

        logging.debug("Sending with " + _cmd)
        res = subprocess.run(_cmd, shell=True)
        if res.returncode != 0:
            logging.error("Send failed: " + str(res.stderr))
            exit(1)


def read_config(_config_file):
    with open(_config_file) as f:
        _c = yaml.safe_load(f)

    if _c['dataPool'] is None:
        logging.error("Missing dataPool in " + config_file)
        exit(1)

    if _c['datasets'] is None:
        logging.error("Missing datasets in " + config_file)
        exit(1)

    if _c['backupPool'] is None:
        logging.error("Missing backupPool in " + config_file)
        exit(1)

    if _c['daily'] is None:
        logging.error("Missing daily in " + config_file)
        exit(1)
    _daily = int(_c['daily'])

    if _c['weekly'] is None:
        logging.error("Missing weekly in " + config_file)
        exit(1)
    _weekly = int(_c['weekly'])

    if _c['monthly'] is None:
        logging.error("Missing monthly in " + config_file)
        exit(1)
    _monthly = int(_c['monthly'])

    if _c['yearly'] is None:
        logging.error("Missing monthly in " + config_file)
        exit(1)
    _yearly = int(_c['yearly'])

    if _c['logFile'] is None:
        logging.error("Missing logFile in " + config_file)
        exit(1)
    _logFile = _c['logFile']

    return Config(pool=_c['dataPool'],
                  backup_pool=_c['backupPool'],
                  datasets=_c['datasets'],
                  num_backups_daily=_daily,
                  num_backups_weekly=_weekly,
                  num_backups_monthly=_monthly,
                  nums_backup_yearly=_yearly,
                  _log_file=_logFile)


def usage():
    basename = os.path.basename(sys.argv[0])

    print('''\
{name} [<options>] <config_file> <schedule>
    
  Options:
   -h  print this usage
   -y  don't ask when creating/overwriting folders in backup
       (caution: intended to be used on initial backups)       
   -c <config_file>  YAML file with the configuration
   -b <backup>  Type of the backup  
   --debug Enable debug logging
   --logToConsole  Log to console and not into file     
    \
    '''.format(name=basename))


def zfs(*args):
    _c = ["zfs"] + list(args)
    logging.debug("Executing " + str(_c))
    _res = subprocess.run(["zfs"] + list(args), capture_output=True, shell=False)
    logging.debug("return_code: " + str(_res.returncode))
    logging.debug("------- STDOUT ----------------")
    logging.debug(_res.stdout)
    logging.debug("------- STDERR ----------------")
    logging.debug(_res.stderr)

    if _res.returncode != 0:
        logging.error("zfs " + str(args) + " failed: " + str(_res.stderr))
        exit(1)


def zfs_destroy(_name):
    zfs("destroy", _name)
    logging.info(_name + " deleted")


def zfs_send(_src, _target, _old=None):
    if _old is None:
        _cmd = "zfs send '" + _src + "' | zfs recv -F '" + _target + "'"
    else:
        _cmd = "zfs send -i '" + _old + "' '" + _src + "' | zfs recv -F '" + _target + "'"

    logging.debug("Executing: " + (str(_cmd)))
    res = subprocess.run(_cmd, shell=True)
    if res.returncode != 0:
        logging.error("Send failed: " + str(res.stderr))
        exit(1)


def get_snapshots(_config: Config, _dataset):
    fqn = _config.pool + "/" + _dataset
    res = subprocess.run(["zfs", "list", "-t", "snapshot", "-H", "-o", "name", fqn],
                         capture_output=True)
    if res.returncode != 0:
        logging.error("Could not determine snapshots of " + fqn)
        logging.error("STD_ERR:" + str(res.stderr))
        exit(1)

    regex = re.compile("^.*@([a-z]+)-(" + DATE_TIME_PATTERN + ")$")

    snaps = []
    for line in res.stdout.decode("utf-8").splitlines():
        result = regex.match(line)
        if result is not None:
            _backup_type = result.group(1)
            _backup_time = datetime.strptime(result.group(2), DATE_TIME_FORMAT)
            snaps.append(Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, _backup_time))

    return snaps


def backup(_config: Config, _dataset: str, _backup_type: str):
    _num_backups_to_keep = _config.get_num_backups(_backup_type)

    logging.info("Backup [{pool}]/{dataset}@{backup_type}] to {backup_pool} (num backups to keep={num_backups_to_keep})"
                 .format(pool=_config.pool, dataset=_dataset, backup_type=_backup_type, backup_pool=_config.backup_pool,
                         num_backups_to_keep=_num_backups_to_keep))

    _all_snapshots = get_snapshots(_config, _dataset)

    sorted(_all_snapshots, key=lambda snapshot: snapshot.backup_time, reverse=True)

    _snapshots = []
    for snap in _all_snapshots:
        if snap.backup_type == _backup_type:
            _snapshots.append(snap)

    logging.info("Existing snapshots: " + str(_snapshots))

    _last_snapshot = None
    if len(_all_snapshots) > 0:
        _last_snapshot = _all_snapshots[len(_all_snapshots) - 1]
    logging.info("Last snapshots: " + str(_last_snapshot))

    while len(_snapshots) > _num_backups_to_keep - 1:
        _snapshots.pop(0).destroy()

    if len(_snapshots) == 0:
        new_snap = Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, datetime.now())
        new_snap.create(_last_snapshot)
    else:
        new_snap = Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, datetime.now())
        new_snap.create(_last_snapshot)


if __name__ == '__main__':
    yesMode = 0
    config_file = None
    backup_type = None

    log_level = logging.INFO
    log_to_console = False

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "-h":
            usage()
            exit(0)
        elif arg == "-s":
            silent = 1
        elif arg == "y":
            yesMode = 1
        elif arg == "-c":
            i = i + 1
            if i >= len(sys.argv):
                logging.error("Missing <config_file> for -c")
                exit(1)
            config_file = sys.argv[i]
        elif arg == '-b':
            i = i + 1
            if i >= len(sys.argv):
                logging.error("Missing <backup> for -t")
                exit(1)
            backup_type = sys.argv[i]
        elif arg == "--debug":
            log_level = logging.DEBUG
        elif arg == "--logToConsole":
            log_to_console = True
        else:
            logging.error("Unknown option [" + arg + "]")
            exit(1)
        i = i + 1

    if config_file is None:
        logging.error("Missing option -c")
        exit(0)

    if backup_type is None:
        logging.error("Missing option -b")
        exit(1)

    config = read_config(config_file)

    if log_to_console:
        logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=log_level, filename=config.log_file,
                            format='%(asctime)s | %(levelname)s | %(message)s')

    logging.info("Backup started --------------------------------")

    for dataset in config.datasets:
        backup(config, dataset, backup_type)

    logging.info("Backup finished --------------------------------")