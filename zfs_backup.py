#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# Created By  : Richard Hierlmeier
# =============================================================================
"""zfs_backup.py: Create backups like rsnapshot with zfs.
__author__      = "Richard Hierlmeier"
__copyright__   = "Copyright 2024, Richard Hierlmeier"
__license__ = "GPL-3.0"
__status__ = "Beta"
"""
# =============================================================================
# Imports
# =============================================================================
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from typing import List

import filelock
import yaml

# =============================================================================
# Constants
# =============================================================================

# Regex to parse a datetime in format YYYY-MM-DD HH:mm:ss
DATE_TIME_PATTERN = "\\d{4}-[0-1]\\d-[0-3]\\dT[0-2]\\d:[0-6]\\d:[0-6]\\d"

# Format string for converting a datetime object into a string
DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


class Config:

    def __init__(self, pool: str, backup_pool: str, datasets: List[str], num_backups_daily: int,
                 num_backups_weekly: int,
                 num_backups_monthly: int, nums_backup_yearly: int, _log_file: str, _run_file: str, _mail_to: str,
                 _prefix: str):
        self.pool = pool
        self.backup_pool = backup_pool
        self.datasets = datasets
        self.num_backups_daily = num_backups_daily
        self.num_backups_weekly = num_backups_weekly
        self.num_backups_monthly = num_backups_monthly
        self.nums_backups_yearly = nums_backup_yearly
        self.log_file = _log_file
        self.run_file = _run_file
        self.mail_to = _mail_to
        self.prefix = _prefix

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
    def __init__(self, _pool, _dataset, _backup_pool, _backup_type, _backup_time, _prefix):
        self.pool = _pool
        self.dataset = _dataset
        self.backup_pool = _backup_pool
        self.backup_type = _backup_type
        self.backup_time = _backup_time
        self.prefix = _prefix

    def __repr__(self):
        return self.get_full_qualified_snap_name()

    def destroy(self):
        zfs_destroy(self.get_full_qualified_snap_name())
        zfs_destroy(self.get_full_qualified_backup_snap_name())

    def get_full_qualified_backup_dataset(self):
        return self.backup_pool + "/" + self.dataset

    def get_snap_name(self):
        return self.prefix + "-" + self.backup_type + "-" + self.backup_time.strftime(DATE_TIME_FORMAT)

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
    with open(_config_file) as _filehandle:
        _c = yaml.safe_load(_filehandle)

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

    if _c['runFile'] is None:
        logging.error("Missing runFile in " + config_file)
        exit(1)
    _runFile = _c['runFile']

    if _c['prefix'] is None:
        logging.error("Missing prefix in " + config_file)
        exit(1)
    _prefix = _c['prefix']

    if _c['mailTo'] is None:
        logging.error("Missing mailTo in " + config_file)
        exit(1)
    _mail_to = _c['mailTo']

    return Config(pool=_c['dataPool'],
                  backup_pool=_c['backupPool'],
                  datasets=_c['datasets'],
                  num_backups_daily=_daily,
                  num_backups_weekly=_weekly,
                  num_backups_monthly=_monthly,
                  nums_backup_yearly=_yearly,
                  _log_file=_logFile,
                  _run_file=_runFile,
                  _mail_to=_mail_to,
                  _prefix=_prefix)


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

    _res = exec_cmd_and_exit_on_error("zfs", "list", "-t", "snapshot", "-H", "-o", "name", fqn)

    regex = re.compile("^.*@([a-z_]+)-([a-z_]+)-(" + DATE_TIME_PATTERN + ")$")

    snaps = []
    for line in _res.stdout.decode("utf-8").splitlines():
        result = regex.match(line)
        if result is not None:
            _prefix = result.group(1)
            _backup_type = result.group(2)
            _backup_time = datetime.strptime(result.group(2), DATE_TIME_FORMAT)
            snaps.append(
                Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, _backup_time, _prefix))

    return snaps


class ZPoolStatus:
    def __init__(self, _name: str, _available=False, _health="UNAVAIL", _capacity_in_percent=0.0):
        self.name = _name
        self.available = _available
        self.health = _health
        self.capacity_in_percent = _capacity_in_percent


def get_zpool_status(_pool_name: str) -> ZPoolStatus:
    _res = exec_cmd("zpool", "get", "-H", "-p", "health,capacity", _pool_name)

    _ret = ZPoolStatus(_pool_name)

    if _res.returncode != 0:
        return _ret

    _ret.available = True

    regex = re.compile("^" + _pool_name + "\\s+([^\\s]+)\\s+([^\\s]+)\\s+")

    for line in _res.stdout.decode("utf-8").splitlines():
        result = regex.match(line)
        if result is not None:
            _property_name = result.group(1)
            _property_value = result.group(2)
            if _property_name == "health":
                # Possible Values ONLINE, DEGRADED, FAULTED, OFFLINE, REMOVED, UNAVAIL
                _ret.health = _property_value
            elif _property_name == "capacity":
                _ret.capacity_in_percent = float(_property_value)
        else:
            logging.debug("Line " + line + " does not match " + str(regex))

    return _ret


def zpool_import(_pool_name: str):
    logging.info("Importing ZFS pool " + _pool_name)
    exec_cmd_and_exit_on_error("zpool", "import", _pool_name)


def exec_cmd_and_exit_on_error(*_cmd):
    logging.debug("Running " + str(_cmd))
    _res = subprocess.run(_cmd, capture_output=True, shell=False)
    _return_code = _res.returncode
    logging.debug("return_code: " + str(_return_code))
    logging.debug("------- STDOUT ----------------")
    logging.debug(_res.stdout)
    logging.debug("------- STDERR ----------------")
    logging.debug(_res.stderr)

    _res = subprocess.run(_cmd, capture_output=True, shell=False)

    if _return_code != 0:
        logging.error("Command [" + str(_cmd) + "] exited with return code " + str(_return_code)
                      + "\n: stderr:" + str(_res.stderr))
        exit(1)

    return _res


def exec_cmd(executable: str, *_args):
    _cmd = [executable] + list(_args)
    logging.debug("Running " + str(_cmd))
    _res = subprocess.run(_cmd, capture_output=True, shell=False)
    logging.debug("return_code: " + str(_res.returncode))
    logging.debug("------- STDOUT ----------------")
    logging.debug(_res.stdout)
    logging.debug("------- STDERR ----------------")
    logging.debug(_res.stderr)
    return _res


def zpool_export(_pool_name: str):
    logging.info("Exporting ZFS pool " + _pool_name)
    exec_cmd_and_exit_on_error("zpool", "export", _pool_name)


def backup(_config: Config, _dataset: str, _backup_type: str):
    _num_backups_to_keep = _config.get_num_backups(_backup_type)

    logging.info("Backup [{pool}]/{dataset}@{backup_type}] to {backup_pool} (num backups to keep={num_backups_to_keep})"
                 .format(pool=_config.pool, dataset=_dataset, backup_type=_backup_type, backup_pool=_config.backup_pool,
                         num_backups_to_keep=_num_backups_to_keep))

    _poolStatus = get_zpool_status(_config.pool)

    if not _poolStatus.available:
        zfs_backup_failed(_config, "zpool " + _config.pool + " is not available")

    if _poolStatus.health != "ONLINE":
        zfs_backup_failed(_config, "zpool " + _config.pool + " is not healthy (health=" + _poolStatus.health + ")")

    _backupPoolStatus = get_zpool_status(_config.backup_pool)

    _backupPoolImport = False

    if not _backupPoolStatus.available:
        zpool_import(_config.backup_pool)
        _backupPoolImport = True
        _backupPoolStatus = get_zpool_status(_config.backup_pool)

    if _backupPoolStatus.health != "ONLINE":
        zfs_backup_failed(_config,
                          "backup zpool " + _config.backup_pool + " is not healthy (health="
                          + _backupPoolStatus.health + ")")

    if _backupPoolStatus.capacity_in_percent < 0.2:
        zfs_backup_warn(_config, "Low capacity " + str(_backupPoolStatus.capacity_in_percent)
                        + " in pool " + _backupPoolStatus.name)

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
        new_snap = Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, datetime.now(), _config.prefix)
        new_snap.create(_last_snapshot)
    else:
        new_snap = Snapshot(_config.pool, dataset, _config.backup_pool, _backup_type, datetime.now(), _config.prefix)
        new_snap.create(_last_snapshot)

    if _backupPoolImport:
        zpool_export(_config.backup_pool)


def sendmail(_config: Config, body, subject):
    sendmail_location = "/usr/sbin/sendmail"  # sendmail location
    p = os.popen("%s -t" % sendmail_location, "w")
    p.write("To: %s\n" % "news@hierlmeier.de")
    p.write("Subject: " + subject + "\n")
    p.write("\n")  # blank line separating headers from body
    p.write(body)
    status = p.close()
    if status != 0:
        print("Sendmail exit status", status)


def zfs_backup_failed(_config: Config, _msg: str):
    logging.error(_msg)
    sendmail(_config, """ZFS backup failed 
   {msg}
   
   """.format(msg=_msg), "ZFS backup failed")
    exit(1)


def zfs_backup_warn(_config: Config, _msg: str):
    logging.warning(_msg)
    sendmail(_config, """Warning from ZFS backup 
   {msg}

   """.format(msg=_msg), "Warning from ZFS backup")


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

    _pid = os.getpid()
    _lock = filelock.FileLock(config.run_file)

    with _lock.acquire():
        with open(config.run_file, "w") as _f:
            _f.write("{pid}\n".format(pid=_pid))

        logging.info("Backup started (pid=%s, backup_type=%s) --------------------------------", _pid, backup_type)

        for dataset in config.datasets:
            backup(config, dataset, backup_type)

        with open(config.run_file, "w") as _f:
            _f.write("")

        logging.info("Backup finished --------------------------------")
