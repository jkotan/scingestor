#   This file is part of scingestor - Scientific Catalog Dataset Ingestor
#
#    Copyright (C) 2021-2021 DESY, Jan Kotanski <jkotan@mail.desy.de>
#
#    nexdatas is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    nexdatas is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with scingestor.  If not, see <http://www.gnu.org/licenses/>.
#
import os
import time
import signal
import sys
import json
import threading
import argparse
import queue
import inotifyx

from .scanDirWatcher import ScanDirWatcher
from .safeINotifier import SafeINotifier
from .datasetIngestor import DatasetIngestor
from .configuration import load_config
from .logger import get_logger, init_logger


class BeamtimeWatcher:
    """ Beamtime Watcher
    """

    def __init__(self, options):
        """ constructor

        :param options: parser options
        :type options: :class:`argparse.Namespace`
        """

        #: (:obj:`bool`) running loop flag
        self.running = True

        signal.signal(signal.SIGTERM, self._signal_handle)

        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = {}
        if options.config:
            self.__config = load_config(options.config) or {}
            get_logger().debug("CONFIGURATION: %s" % str(self.__config))

        #: (:obj:`list` <:obj:`str`>) beamtime directories
        self.__beamtime_dirs = [
            # "/gpfs/current",
            # "/gpfs/commissioning",
        ]
        if "beamtime_dirs" in self.__config.keys() \
           and isinstance(self.__config["beamtime_dirs"], list):
            self.__beamtime_dirs = self.__config["beamtime_dirs"]

        #: (:obj:`str`) beamtime base directories
        self.__beamtime_base_dir = ""
        if "beamtime_base_dir" in self.__config.keys() \
           and self.__config["beamtime_base_dir"]:
            self.__beamtime_base_dir = os.path.abspath(
                self.__config["beamtime_base_dir"])

        #: (:obj:`bool`) access groups from proposals
        self.__groups_from_proposal = False
        if "owner_access_groups_from_proposal" in self.__config.keys():
            self.__groups_from_proposal = \
                self.__config["owner_access_groups_from_proposal"]

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>)
        #                             beamtime path to watcher path map
        self.__wait_for_dirs = {}

        #: (:obj:`int`) notifier id
        self.__notifier = None
        #: (:obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.__wd_to_path = {}
        #: (:obj:`dict` <:obj:`int`, :obj:`str`>)
        #                               beamtime watch description paths
        self.__wd_to_queue = {}
        #: (:obj:`dict` <:obj:`int`, :class:`queue.Queue`>)
        #                               beamtime watch description paths
        self.__wd_to_bpath = {}
        #: (:obj:`dict` <:obj:`int`, :class:`queue.Queue`>)
        #                               beamtime watch description paths
        self.__wd_to_bqueue = {}

        #: (:obj:`str`) beamtime file prefix
        self.__bt_prefix = "beamtime-metadata-"
        #: (:obj:`str`) beamtime file postfix
        self.__bt_postfix = ".json"

        #: (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #                :class:`scanDirWatcher.ScanDirWatcher`>)
        #        scandir watchers instances for given path and beamtime file
        self.__scandir_watchers = {}
        #: (:class:`threading.Lock`) scandir watcher dictionary lock
        self.__scandir_lock = threading.Lock()
        #: (:obj:`float`) timeout value for inotifyx get events
        self.__timeout = 0.01

        if "get_event_timeout" in self.__config.keys():
            try:
                self.__timeout = float(self.__config["get_event_timeout"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        try:
            #: (:obj:`float`) run time in s
            self.__runtime = float(options.runtime)
        except Exception:
            self.__runtime = 0
        #: (:obj:`float`) start time in s
        self.__starttime = time.time()

        if "beamtime_filename_prefix" in self.__config.keys():
            self.__bt_prefix = self.__config["beamtime_filename_prefix"]

        if "beamtime_filename_postfix" in self.__config.keys():
            self.__bt_postfix = self.__config["beamtime_filename_postfix"]

        if not self.__beamtime_dirs and not self.__beamtime_base_dir:
            self.running = False
            get_logger().warning(
                'BeamtimeWatcher: Beamtime directories not defined')

    def _find_bt_files(self, path, prefix, postfix):
        """ find beamtime files with given prefix and postfix in the given path

        :param path: beamtime directory
        :type path: :obj:`str`
        :param prefix: file name prefix
        :type prefix: :obj:`str`
        :param postfix: file name postfix
        :type postfix: :obj:`str`
        :returns: list of found files
        :rtype: :obj:`list` <:obj:`str`>
        """
        files = []
        try:
            if os.path.isdir(path):
                files = [fl for fl in os.listdir(path)
                         if (fl.startswith(prefix)
                             and fl.endswith(postfix))]
        except Exception as e:
            get_logger().warning(str(e))
        return files

    def _start_notifier(self, paths, bpath=None):
        """ start notifier for all given paths to look for beamtime files

        :param paths: beamtime file paths
        :type paths: :obj:`list` <:obj:`str`>
        :param bpath: beamtime base path
        :type bpath: :obj:`str`
        """
        self.__notifier = SafeINotifier()
        if "inotify_timeout" in self.__config.keys():
            try:
                self.__notifier.inotify_timeout = float(
                    self.__config["inotify_timeout"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if bpath:
            self._add_base_path(bpath, split=False)
        for path in paths:
            self._add_path(path)

    def _add_path(self, path):
        """ add path to beamtime notifier to look for beamtime files

        :param path: beamtime file path
        :type path: :obj:`str`
        """
        try:
            wqueue, watch_descriptor = self.__notifier.add_watch(
                path,
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.__wd_to_path[watch_descriptor] = path
            self.__wd_to_queue[watch_descriptor] = wqueue
            get_logger().info('BeamtimeWatcher: Adding watch %s: %s'
                              % (str(watch_descriptor), path))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))
            self._add_base_path(path)

    def _add_base_path(self, path, split=True):
        """ add base path to notifier

        :param path: base file path
        :type path: :obj:`str`
        :param split: split base file path
        :type split: :obj:`bool`
        """
        failing = True
        bpath = path
        while failing:
            try:
                if split:
                    bpath, _ = os.path.split(bpath)

                if not bpath:
                    bpath = os.path.abspath()
                bqueue, watch_descriptor = self.__notifier.add_watch(
                    bpath,
                    inotifyx.IN_CREATE | inotifyx.IN_CLOSE_WRITE
                    | inotifyx.IN_MOVED_TO
                    | inotifyx.IN_MOVE_SELF
                    | inotifyx.IN_DELETE
                    | inotifyx.IN_ALL_EVENTS
                )
                failing = False
                self.__wd_to_bpath[watch_descriptor] = bpath
                self.__wd_to_bqueue[watch_descriptor] = bqueue
                self.__wait_for_dirs[bpath] = path
                get_logger().info('BeamtimeWatcher: '
                                  'Adding base watch %s: %s'
                                  % (str(watch_descriptor), bpath))
            except Exception as e:
                get_logger().warning('%s: %s' % (bpath, str(e)))
                if bpath == '/':
                    failing = False

    def _stop_notifier(self):
        """ stop notifier
        """
        for wd in list(self.__wd_to_path.keys()):
            self.__notifier.rm_watch(wd)
            path = self.__wd_to_path.pop(wd)
            self.__wd_to_queue.pop(wd)
            get_logger().info('BeamtimeWatcher: '
                              'Removing watch %s: %s' % (str(wd), path))
        for wd in list(self.__wd_to_bpath.keys()):
            self.__notifier.rm_watch(wd)
            path = self.__wd_to_bpath.pop(wd)
            self.__wd_to_bqueue.pop(wd)
            get_logger().info('BeamtimeWatcher: '
                              'Removing base watch %s: %s' % (str(wd), path))

    def start(self):
        """ start beamtime watcher
        """
        try:

            # find already existing beamtime dirs in the base directory
            if self.__beamtime_base_dir and \
               os.path.isdir(self.__beamtime_base_dir):
                subdirs = [
                    it.path for it in os.scandir(self.__beamtime_base_dir)
                    if it.is_dir()]
                for sdir in subdirs:
                    asdir = os.path.abspath(sdir)
                    if asdir not in self.__beamtime_dirs:
                        self.__beamtime_dirs.append(asdir)

            # start beamtime file notifiers
            self._start_notifier(
                self.__beamtime_dirs, self.__beamtime_base_dir)

            # find already existing beamtime files
            for path in self.__beamtime_dirs:
                files = self._find_bt_files(
                    path, self.__bt_prefix, self.__bt_postfix)

                # run ScanDirWatcher for each beamtime file subdirectory
                self._launch_scandir_watcher(path, files)
                get_logger().debug('Files of %s: %s' % (path, files))

            while self.running:
                get_logger().debug('Bt Tic')
                if not self.__wd_to_queue and not self.__wd_to_bqueue:
                    time.sleep(self.__timeout/10.)
                for qid in list(self.__wd_to_queue.keys()):
                    wqueue = self.__wd_to_queue[qid]
                    try:
                        try:
                            timeout = self.__timeout \
                                / len(self.__wd_to_queue)
                        except Exception:
                            timeout = self.__timeout
                        event = wqueue.get(block=True, timeout=timeout)
                    except queue.Empty:
                        break
                    if qid in self.__wd_to_path.keys():
                        get_logger().debug(
                            'Bt: %s %s %s' % (event.name,
                                              event.masks,
                                              self.__wd_to_path[qid]))
                        # get_logger().info(
                        #     'Bt: %s %s %s' % (event.name,
                        #                       event.masks,
                        #                       self.__wd_to_path[qid]))
                        masks = event.masks.split("|")
                        if "IN_IGNORED" in masks or \
                           "IN_MOVE_FROM" in masks or \
                           "IN_DELETE" in masks or \
                           "IN_MOVE_SELF" in masks:
                            # path/file does not exist anymore
                            #     (moved/deleted)
                            path = self.__wd_to_path.pop(qid)
                            self.__wd_to_queue.pop(qid)
                            # get_logger().info(
                            #     'BeamtimeWatcher: '
                            #     'Removing watch on a IMDM event %s: %s'
                            #     % (str(qid), path))
                            get_logger().debug('Removed %s' % path)
                            ffn = os.path.abspath(path)
                            dds = []
                            get_logger().debug(
                                'ScanDirs watchers: %s' %
                                (str(list(self.__scandir_watchers.keys()))))
                            with self.__scandir_lock:
                                get_logger().debug('ScanDirs in lock')
                                for ph, fl in \
                                        list(self.__scandir_watchers.keys()):
                                    if ffn == fl or ph == ffn:
                                        get_logger().debug(
                                            'POP Scandir watchers: %s %s' %
                                            (ph, fl))
                                        # stop scandir watcher if running
                                        ds = self.__scandir_watchers.pop(
                                            (ph, fl))
                                        ds.running = False
                                        dds.append(ds)
                            get_logger().debug(
                                'stopping ScanDirs %s' % str(dds))
                            while len(dds):
                                ds = dds.pop()
                                ds.running = False
                                get_logger().debug("Joining ScanDirWatcher")
                                ds.join()
                                get_logger().debug("ScanDirWatcher Joined")
                            get_logger().debug('add paths')
                            self._add_path(path)

                        elif "IN_CREATE" in masks or \
                             "IN_MOVE_TO" in masks or \
                             "IN_CLOSE_WRITE" in masks:

                            files = [fl for fl in [event.name]
                                     if (fl.startswith(self.__bt_prefix) and
                                         fl.endswith(self.__bt_postfix))]
                            if files:
                                # new beamtime file
                                self._launch_scandir_watcher(
                                    self.__wd_to_path[qid], files)
                            else:
                                path = self.__wd_to_path.pop(qid)
                                self.__wd_to_queue.pop(qid)
                                get_logger().debug("POP path: %s" % path)
                                # get_logger().info(
                                #     'BeamtimeWatcher: '
                                #     'Removing watch on a CM event %s: %s'
                                #     % (str(qid), path))
                                files = self._find_bt_files(
                                    path, self.__bt_prefix, self.__bt_postfix)

                                self._launch_scandir_watcher(path, files)

                            get_logger().debug(
                                'Start beamtime %s' % event.name)
                        # elif "IN_DELETE" in masks or \
                        #      "IN_MOVE_MOVE" in masks:
                        #     " remove scandir_watcher "

                for qid in list(self.__wd_to_bqueue.keys()):
                    bqueue = self.__wd_to_bqueue[qid]
                    try:
                        try:
                            timeout = self.__timeout \
                                / len(self.__wd_to_bqueue)
                        except Exception:
                            timeout = self.__timeout
                        event = bqueue.get(block=True, timeout=timeout)
                    except queue.Empty:
                        break
                    if qid in self.__wd_to_bpath.keys():
                        get_logger().debug(
                            'BB: %s %s %s' % (event.name,
                                              event.masks,
                                              self.__wd_to_bpath[qid]))
                        # get_logger().info(
                        #     'BB: %s %s %s' % (event.name,
                        #                       event.masks,
                        #                       self.__wd_to_bpath[qid]))
                        masks = event.masks.split("|")
                        if not self.__beamtime_base_dir:
                            # if event.name is not None:
                            bpath = self.__wd_to_bpath.pop(qid)
                            # npath = os.path.join(bpath, event.name)
                            if "IN_IGNORED" not in \
                               event.masks.split():
                                self.__notifier.rm_watch(qid)
                            path = self.__wait_for_dirs.pop(bpath)
                            self._add_path(path)
                        elif "IN_ISDIR" in masks and (
                                "IN_CREATE" in masks or "IN_MOVE_TO" in masks):
                            if event.name:
                                dr = os.path.abspath(os.path.join(
                                    self.__wd_to_bpath[qid], event.name))
                            else:
                                dr = os.path.abspath(self.__wd_to_bpath[qid])
                            bpath, btdir = os.path.split(dr)
                            if bpath == self.__beamtime_base_dir and \
                               os.path.isdir(dr):
                                if dr not in self.__beamtime_dirs:
                                    self.__beamtime_dirs.append(dr)
                                    self._add_path(dr)
                                    files = self._find_bt_files(
                                        dr, self.__bt_prefix,
                                        self.__bt_postfix)
                                    self._launch_scandir_watcher(dr, files)

                                    get_logger().debug(
                                        'Files of %s: %s' % (dr, files))

                get_logger().debug(
                    "Running: %s s" % (time.time() - self.__starttime))
                if self.__runtime and \
                   time.time() - self.__starttime > self.__runtime:
                    self.stop()
        except KeyboardInterrupt:
            get_logger().warning('Keyboard interrupt (SIGINT) received...')
            self.stop()

    def _launch_scandir_watcher(self, path, files):
        """ launch scandir watcher

        :param path: base file path
        :type path: :obj:`str`
        :param path: beamtime files
        :type path: :obj:`list`<:obj:`str`>
        """
        for bt in files:
            ffn = os.path.abspath(os.path.join(path, bt))
            try:
                sdw = None
                with self.__scandir_lock:
                    try:
                        with open(ffn) as fl:
                            btmd = json.loads(fl.read())
                    except Exception:
                        time.sleep(0.1)
                        with open(ffn) as fl:
                            btmd = json.loads(fl.read())
                    if (path, ffn) not in self.__scandir_watchers.keys():
                        get_logger().info(
                            'BeamtimeWatcher: Create ScanDirWatcher %s %s'
                            % (path, ffn))
                        btmd = self.__append_proposal_groups(btmd, path)
                        sdw = self.__scandir_watchers[(path, ffn)] =  \
                            ScanDirWatcher(self.__config, path, btmd, ffn)
                if sdw is not None:
                    sdw.start()
            except Exception as e:
                get_logger().warning(
                    "%s cannot be watched: %s" % (ffn, str(e)))

    def __append_proposal_groups(self, meta, path):
        """ appends owner and access groups to beamtime

        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param path: base file path
        :type path: :obj:`str`
        :returns: updated beamtime configuration
        :rtype: :obj:`dict` <:obj:`str`, `any`>
        """
        if not self.__groups_from_proposal or (
                "accessGroups" in meta and "ownerGroup" in meta):
            return meta
        else:
            ingestor = DatasetIngestor(
                self.__config, path, "", "", meta, path)
            return ingestor.append_proposal_groups()

    def stop(self):
        """ stop beamtime watcher
        """
        get_logger().debug('Cleaning up...')
        self.running = False
        time.sleep(0.2)
        self._stop_notifier()
        with self.__scandir_lock:
            for pf, dsw in self.__scandir_watchers.items():
                path, ffn = pf
                get_logger().info('BeamtimeWatcher: '
                                  'Stopping ScanDirWatcher %s' % ffn)
                dsw.running = False
                dsw.join()
                #     sys.exit(0)
            self.__scandir_watchers = []

    def _signal_handle(self, sig, _):
        """ handle SIGTERM

        :param sig: signal name, i.e. 'SIGINT', 'SIGHUP', 'SIGALRM', 'SIGTERM'
        :type sig: :obj:`str`
        """
        get_logger().warning('SIGTERM received...')
        self.stop()


def main():
    """ the main program function
    """

    description = "BeamtimeWatcher service SciCat Dataset ingestior"

    epilog = "" \
        " examples:\n" \
        "      scicat_dataset_ingestor -c ~/.scingestor.yaml\n " \
        "      scicat_dataset_ingestor -c ~/.scingestor.yaml -l debug\n" \
        "\n"
    parser = argparse.ArgumentParser(
        description=description, epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--configuration", dest="config",
        help="configuration file name")
    parser.add_argument(
        "-r", "--runtime", dest="runtime",
        help=("stop program after runtime in seconds"))
    parser.add_argument(
        "-l", "--log", dest="log",
        help="logging level, i.e. debug, info, warning, error, critical",
        default="info")
    try:
        options = parser.parse_args()
    except Exception as e:
        sys.stderr.write("Error: %s\n" % str(e))
        sys.stderr.flush()
        parser.print_help()
        print("")
        sys.exit(255)

    init_logger("SciCatDatasetIngestor", options.log)

    bw = BeamtimeWatcher(options)
    bw.start()
    SafeINotifier().stop()
    sys.exit(0)
