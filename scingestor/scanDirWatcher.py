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
import threading
import time
import queue
import socket
import pathlib

from .datasetWatcher import DatasetWatcher
from .safeINotifier import SafeINotifier
from .pathConverter import PathConverter
from .logger import get_logger

import inotifyx


class ScanDirWatcher(threading.Thread):
    """ ScanDir Watcher
    """
    def __init__(self,
                 configuration,
                 path, meta, beamtimefile, depth):
        """ constructor

        :param configuration: dictionary with the ingestor configuration
        :type configuration: :obj:`dict` <:obj:`str`, `any`>
        :param path: scan dir path
        :type path: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param beamtimefile: beamtime file
        :type beamtimefile: :obj:`str`
        :param depth: scandir depth level
        :type depth: :obj:`int`
        """
        threading.Thread.__init__(self)

        #: (:obj:`bool`) running loop flag
        self.running = True

        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}

        #: (:obj:`str`) home directory
        self.__homepath = str(pathlib.Path.home())

        #: (:obj:`str`) core path
        self.__corepath = meta.get("corePath", None)

        #: (:obj:`str`) beamtime path
        self.__bpath = os.path.split(beamtimefile)[0]

        #: (:obj:`bool`) use core path
        self.__usecorepath = False
        if "use_corepath_as_scandir" in self.__config.keys():
            self.__usecorepath = bool(
                self.__config["use_corepath_as_scandir"])

        #: (:obj:`bool`) watch scandir subdirectories
        self.__watchscandirsubdir = False
        if "watch_scandir_subdir" in self.__config.keys():
            self.__watchscandirsubdir = bool(
                self.__config["watch_scandir_subdir"])

        #: (:class:`scingestor.datasetWatcher.DatasetWatcher`) use core path
        self.__conv = PathConverter(
            self.__corepath, self.__bpath,
            self.__usecorepath and self.__corepath)
        #: (:obj:`str`) scan dir path
        self.__path = self.__conv.to_core(path)
        #: (:obj:`str`) beamtime core path and file name
        self.__btfile = self.__conv.to_core(beamtimefile)
        #: (:obj:`dict` <:obj:`str`, `any`>) beamtime configuration
        self.__meta = meta
        #: (:obj:`int`) scan dir depth
        self.__depth = depth
        #: (:obj:`str`) beamtime id
        self.__beamtimeId = meta["beamtimeId"]

        #: (:obj:`str`) scicat dataset file pattern
        self.__ds_pattern = "scicat-datasets-{beamtimeid}.lst"
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__ids_pattern = "scicat-ingested-datasets-{beamtimeid}.lst"
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__hostname = socket.gethostname()

        #: (:obj:`int`) notifier ID
        self.__notifier = None
        #: (obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.__wd_to_path = {}
        #: (:obj:`dict` <:obj:`int`, :obj:`str`>)
        #:                              beamtime watch description paths
        self.__wd_to_queue = {}

        #: (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #:               :class:`scanDirWatcher.ScanDirWatcher`>)
        #:       dataset watchers instances for given path and beamtime file
        self.__dataset_watchers = {}
        #: (:class:`threading.Lock`) dataset watcher dictionary lock
        self.__dataset_lock = threading.Lock()
        #: (:obj:`float`) timeout value for inotifyx get events
        self.__timeout = 0.1

        #: (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #:               :class:`scanDirWatcher.ScanDirWatcher`>)
        #:       scandir watchers instances for given path and beamtime file
        self.__scandir_watchers = {}
        #: (:class:`threading.Lock`) scandir watcher dictionary lock
        self.__scandir_lock = threading.Lock()

        #: (:obj:`list` <:obj:`str`>) scandir blacklist
        self.__scandir_blacklist = [
            "/gpfs/current/scratch_bl",
            "/gpfs/current/processed",
            "/gpfs/current/shared"
        ]
        if "scandir_blacklist" in self.__config.keys() \
           and isinstance(self.__config["scandir_blacklist"], list):
            self.__scandir_blacklist = []
            for sdir in self.__config["scandir_blacklist"]:
                if sdir:
                    self.__scandir_blacklist.append(sdir.format(
                        homepath=self.__homepath))

        if "get_event_timeout" in self.__config.keys():
            try:
                self.__timeout = float(self.__config["get_event_timeout"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "datasets_filename_pattern" in self.__config.keys():
            self.__ds_pattern = self.__config["datasets_filename_pattern"]

        if "ingested_datasets_filename_pattern" in self.__config.keys():
            self.__ids_pattern = \
                self.__config["ingested_datasets_filename_pattern"]

        #: (:obj:`str`) datasets file name
        self.__dslist_filename = self.__ds_pattern.format(
            beamtimeid=self.__beamtimeId, hostname=self.__hostname)
        #: (:obj:`str`) ingescted datasets file name
        self.__idslist_filename = self.__ids_pattern.format(
            beamtimeid=self.__beamtimeId, hostname=self.__hostname)
        #: (:obj:`str`) datasets file name
        self.__dslist_fullname = os.path.join(
            self.__path, self.__dslist_filename)

        #: (:obj:`str`) ingestor log directory
        self.__var_dir = ""
        if "ingestor_var_dir" in self.__config.keys():
            self.__var_dir = str(
                self.__config["ingestor_var_dir"]).format(
                    beamtimeid=self.__beamtimeId,
                    homepath=self.__homepath)
        if self.__var_dir == "/":
            self.__var_dir = ""

    def _start_notifier(self, path):
        """ start notifier

        :param path: beamtime file subdirectory
        :type path: :obj:`str`
        """
        self.__notifier = SafeINotifier()
        self._add_path(path)

    def _add_path(self, path):
        """ add path to notifier

        :param path: beamtime file path
        :type path: :obj:`str`
        """
        try:
            wqueue, watch_descriptor = self.__notifier.add_watch(
                self.__conv.from_core(path),
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.__wd_to_path[watch_descriptor] = path
            self.__wd_to_queue[watch_descriptor] = wqueue
            get_logger().info('ScanDirWatcher: Adding watch %s: %s'
                              % (str(watch_descriptor), path))
            # get_logger().info('ScanDirWatcher START %s: %s'
            #                   % (self.__notifier, path))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))

    def _stop_notifier(self):
        """ stop notifier
        """
        for wd in list(self.__wd_to_path.keys()):
            path = self.__wd_to_path.pop(wd, None)
            self.__wd_to_queue.pop(wd, None)
            get_logger().info(
                'ScanDirWatcher: '
                'Removing watch %s: %s' % (str(wd), path))

    def _launch_scandir_watcher(self, paths):
        """ launch scandir watcher

        :param path: list of subdirectories
        :type path: :obj:`list`<:obj:`str`>
        """
        if self.__depth != 0:
            for path in sorted(paths):
                if self.__conv.from_core(path) in self.__scandir_blacklist or \
                   self.__conv.to_core(path) in self.__scandir_blacklist:
                    continue
                sdw = None
                try:
                    with self.__scandir_lock:
                        if (path, self.__btfile) \
                           not in self.__scandir_watchers.keys():
                            sdw = \
                                self.__scandir_watchers[
                                    (path, self.__btfile)] = ScanDirWatcher(
                                        self.__config,
                                        self.__conv.from_core(path),
                                        self.__meta,
                                        self.__conv.from_core(self.__btfile),
                                        self.__depth - 1)
                            get_logger().info(
                                'ScanDirWatcher: Create ScanDirWatcher %s %s'
                                % (path, self.__btfile))
                    if sdw is not None:
                        sdw.start()
                    time.sleep(self.__timeout/10.)
                except Exception as e:
                    get_logger().warning(
                        "%s cannot be watched: %s" % (path, str(e)))

    def run(self):
        """ scandir watcher thread
        """
        try:
            self._start_notifier(self.__path)
            # get_logger().info("START %s " % (self.__notifier))

            get_logger().debug("ScanDir file:  %s " % (self.__dslist_fullname))
            if os.path.isfile(self.__dslist_fullname):
                dw = None
                with self.__dataset_lock:
                    fn = self.__dslist_fullname
                    if fn not in self.__dataset_watchers.keys():
                        ifn = fn[:-(len(self.__dslist_filename))] + \
                            self.__idslist_filename
                        if self.__var_dir:
                            ifn = "%s%s" % (self.__var_dir, ifn)
                        ipath, _ = os.path.split(ifn)
                        if not os.path.isdir(ipath):
                            os.makedirs(ipath, exist_ok=True)
                        dw = self.__dataset_watchers[fn] = DatasetWatcher(
                            self.__config,
                            self.__path,
                            fn, ifn, self.__meta,
                            self.__conv.from_core(self.__btfile))
                        get_logger().info(
                            'ScanDirWatcher: Creating DatasetWatcher %s' % fn)
                if dw is not None:
                    dw.start()
                    # get_logger().info(str(btmd))

            if os.path.isdir(self.__path) and (
                    self.__watchscandirsubdir or not
                    os.path.isfile(self.__dslist_fullname)):
                subdirs = [it.path for it in os.scandir(self.__path)
                           if it.is_dir()]
                self._launch_scandir_watcher(subdirs)

            while self.running:
                get_logger().debug('Dt Tac')
                if not self.__wd_to_queue:
                    time.sleep(self.__timeout/10.)
                for qid in list(self.__wd_to_queue.keys()):
                    wqueue = self.__wd_to_queue[qid]
                    try:
                        event = wqueue.get(block=True, timeout=self.__timeout)
                    except queue.Empty:
                        break
                    if qid in self.__wd_to_path.keys():
                        get_logger().debug(
                            'Sd: %s %s %s %s' % (qid,
                                                 event.name,
                                                 event.masks,
                                                 self.__wd_to_path[qid]))
                        masks = event.masks.split("|")
                        if self.__watchscandirsubdir and \
                                "IN_ISDIR" in masks and (
                                "IN_CREATE" in masks
                                or "IN_MOVE_TO" in masks):
                            npath = os.path.join(
                                self.__wd_to_path[qid], event.name)
                            self._launch_scandir_watcher([npath])
                        elif "IN_IGNORED" in masks or \
                                "IN_MOVE_FROM" in masks or \
                                "IN_DELETE" in masks or \
                                "IN_MOVE_SELF" in masks:
                            # path/file does not exist anymore
                            #     (moved/deleted)
                            if event.name is not None:
                                npath = os.path.join(
                                    self.__wd_to_path[qid], event.name)
                                get_logger().debug(
                                    "Remove path/file %s" % npath)
                                if self.__dslist_fullname == npath and \
                                   not os.path.isfile(self.__dslist_fullname) \
                                   and os.path.isdir(self.__path):
                                    subdirs = [
                                        it.path
                                        for it in os.scandir(self.__path)
                                        if it.is_dir()]
                                    get_logger().debug(
                                        "Sub-directories: %s" % str(subdirs))
                                    self._launch_scandir_watcher(subdirs)
                                    get_logger().debug(
                                        "watcher for subdirectories launched")

                        elif "IN_ISDIR" not in masks and (
                                "IN_CREATE" in masks or "IN_MOVE_TO" in masks):
                            fn = os.path.join(
                                self.__wd_to_path[qid], event.name)
                            dw = None
                            with self.__dataset_lock:
                                if fn not in self.__dataset_watchers.keys() \
                                   and fn == self.__dslist_fullname:
                                    ifn = \
                                        fn[:-(len(self.__dslist_filename))] \
                                        + self.__idslist_filename
                                    if self.__var_dir:
                                        ifn = "%s%s" % (self.__var_dir, ifn)
                                    ipath, _ = os.path.split(ifn)
                                    if not os.path.isdir(ipath):
                                        os.makedirs(ipath, exist_ok=True)
                                    dw = self.__dataset_watchers[fn] = \
                                        DatasetWatcher(
                                            self.__config, self.__path,
                                            fn, ifn,
                                            self.__meta, self.__btfile)
                            if dw is not None:
                                dw.start()
                                get_logger().info(
                                    'ScanDirWatcher: Creating '
                                    'DatasetWatcher %s' % fn)
                            dds = []
                            if not self.__watchscandirsubdir:
                                with self.__dataset_lock:
                                    for path, fn in list(
                                            self.__scandir_watchers.keys()):
                                        ds = self.__scandir_watchers.pop(
                                            (path, fn))
                                        get_logger().info(
                                            'ScanDirWatcher: '
                                            'Stopping ScanDirWatcher %s'
                                            % (fn))
                                        ds.running = False
                                        dds.append(ds)
                                while len(dds):
                                    ds = dds.pop()
                                    ds.join()

                        elif "IN_ISDIR" in masks and (
                                "IN_CREATE" in masks
                                or "IN_MOVE_TO" in masks):
                            if not os.path.isfile(self.__dslist_fullname):
                                npath = os.path.join(
                                    self.__wd_to_path[qid], event.name)
                                self._launch_scandir_watcher([npath])
                        # elif "IN_DELETE_SELF" in masks:
                        #     "remove scandir watcher"
                        #     # self.__wd_to_path[qid]

                # time.sleep(self.__timeout)
        finally:
            get_logger().debug("Stopping ScanDirWatcher")
            self.stop()

    def stop(self):
        """ stop the watcher
        """
        get_logger().debug("Stop ScanDirWatcher")
        self.running = False
        # time.sleep(0.2)
        self._stop_notifier()
        with self.__dataset_lock:
            for fn, scw in self.__dataset_watchers.items():
                get_logger().info(
                    'ScanDirWatcher: Stopping DatasetWatcher %s' % (fn))
                scw.running = False
                scw.join()
        # self.__dataset_watchers = []

        with self.__scandir_lock:
            for pf, dsw in self.__scandir_watchers.items():
                path, fn = pf
                get_logger().info('ScanDirWatcher: '
                                  'Stopping ScanDirWatcher %s' % (fn))
                dsw.running = False
                dsw.join()
        # self.__scandir_watchers = []
