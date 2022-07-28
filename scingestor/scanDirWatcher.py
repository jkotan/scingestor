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


from .datasetWatcher import DatasetWatcher
from .safeINotifier import SafeINotifier
from .logger import get_logger

import inotifyx


class ScanDirWatcher(threading.Thread):
    """ ScanDir Watcher
    """
    def __init__(self,
                 configuration,
                 path, meta, bpath,
                 delay=5):
        """ constructor

        :param configuration: dictionary with the ingestor configuration
        :type configuration: :obj:`dict` <:obj:`str`, `any`>
        :param path: scan dir path
        :type path: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`,`any`>
        :param bpath: beamtime file
        :type bpath: :obj:`str`
        :param delay: time delay
        :type delay: :obj:`int`
        """
        threading.Thread.__init__(self)
        # (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}
        # (:obj:`str`) scan dir path
        self.__path = path
        # (:obj:`str`) beamtime path and file name
        self.__bpath = bpath
        # (:obj:`dict` <:obj:`str`,`any`>) beamtime configuration
        self.__meta = meta
        # (:obj:`str`) beamtime id
        self.beamtimeId = meta["beamtimeId"]
        # (:obj:`str`) beamline metadata
        self.__meta = meta
        # (:obj:`float`) delay time for ingestion in s
        self.delay = delay
        # (:obj:`bool`) running loop flag
        self.running = True
        # (:obj:`str`) scicat dataset file pattern
        self.ds_pattern = "scicat-datasets-{bt}.lst"
        # (:obj:`str`) indested scicat dataset file pattern
        self.ids_pattern = "scicat-ingested-datasets-{bt}.lst"

        # (:obj:`int`) notifier ID
        self.notifier = None
        # (:obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.wd_to_path = {}
        # (:obj:`dict` <:obj:`int`, :obj:`str`>)
        #                               beamtime watch description paths
        self.wd_to_queue = {}

        # (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #                :class:`scanDirWatcher.ScanDirWatcher`>)
        #        dataset watchers instances for given path and beamtime file
        self.dataset_watchers = {}
        # (:class:`threading.Lock`) dataset watcher dictionary lock
        self.dataset_lock = threading.Lock()
        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 0.01

        # (:obj:`dict` <(:obj:`str`, :obj:`str`),
        #                :class:`scanDirWatcher.ScanDirWatcher`>)
        #        scandir watchers instances for given path and beamtime file
        self.scandir_watchers = {}
        # (:class:`threading.Lock`) scandir watcher dictionary lock
        self.scandir_lock = threading.Lock()

        # (:obj:`str`) datasets file name
        self.dslist_filename = self.ds_pattern.format(bt=self.beamtimeId)
        # (:obj:`str`) ingescted datasets file name
        self.idslist_filename = self.ids_pattern.format(bt=self.beamtimeId)
        # (:obj:`str`) datasets file name
        self.dslist_fullname = os.path.join(self.__path, self.dslist_filename)

    def _start_notifier(self, path):
        """ start notifier

        :param path: beamtime file subpath
        :type path: :obj:`str
        """
        self.notifier = SafeINotifier()
        self._add_path(path)

    def _add_path(self, path):
        """ add path to notifier

        :param path: beamtime file path
        :type path: :obj:`str`
        """
        try:
            wqueue, watch_descriptor = self.notifier.add_watch(
                path,
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.wd_to_path[watch_descriptor] = path
            self.wd_to_queue[watch_descriptor] = wqueue
            get_logger().info('ScanDirWatcher: Adding watch %s: %s'
                              % (str(watch_descriptor), path))
            # get_logger().info('ScanDirWatcher START %s: %s'
            #                   % (self.notifier, path))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))

    def _stop_notifier(self):
        """ stop notifier
        """
        for wd in list(self.wd_to_path.keys()):
            path = self.wd_to_path.pop(wd, None)
            self.wd_to_queue.pop(wd, None)
            get_logger().info(
                'ScanDirWatcher: '
                'Removing watch %s: %s' % (str(wd), path))

    def _lunch_scandir_watcher(self, paths):
        """ lunch scandir watcher

        :param path: list of subdirectories
        :type path: :obj:`list`<:obj:`str`>
        """
        for path in paths:
            sdw = None
            try:
                with self.scandir_lock:
                    if (path, self.__bpath) \
                       not in self.scandir_watchers.keys():
                        sdw = \
                            self.scandir_watchers[(path, self.__bpath)] =  \
                            ScanDirWatcher(self.__config,
                                           path, self.__meta, self.__bpath)
                        get_logger().info(
                            'ScanDirWatcher: Create ScanDirWatcher %s %s'
                            % (path, self.__bpath))
                if sdw is not None:
                    sdw.start()
            except Exception as e:
                get_logger().warning(
                    "%s cannot be watched: %s" % (path, str(e)))

    def run(self):
        """ scandir watcher thread
        """
        try:
            self._start_notifier(self.__path)
            # get_logger().info("START %s " % (self.notifier))

            # get_logger().info("ScanDir file:  %s " % (self.dslist_fullname))
            get_logger().debug("ScanDir file:  %s " % (self.dslist_fullname))
            if os.path.isfile(self.dslist_fullname):
                dw = None
                with self.dataset_lock:
                    fn = self.dslist_fullname
                    if fn not in self.dataset_watchers.keys():
                        ifn = fn[:-(len(self.dslist_filename))] + \
                            self.idslist_filename
                        dw = self.dataset_watchers[fn] = DatasetWatcher(
                            self.__config,
                            self.__path, fn, ifn, self.__meta, self.__bpath)
                        get_logger().info(
                            'ScanDirWatcher: Creating DatasetWatcher %s' % fn)
                if dw is not None:
                    dw.start()
                    # get_logger().info(str(btmd))

            elif os.path.isdir(self.__path):
                subdirs = [it.path for it in os.scandir(self.__path)
                           if it.is_dir()]
                self._lunch_scandir_watcher(subdirs)

            while self.running:
                # time.sleep(self.delay)
                get_logger().debug('Dt Tac')
                if not self.wd_to_queue:
                    time.sleep(self.timeout/10.)
                for qid in list(self.wd_to_queue.keys()):
                    wqueue = self.wd_to_queue[qid]
                    try:
                        event = wqueue.get(block=True, timeout=self.timeout)
                    except queue.Empty:
                        break
                    if qid in self.wd_to_path.keys():
                        get_logger().debug(
                            'Sd: %s %s %s %s' % (qid,
                                                 event.name,
                                                 event.masks,
                                                 self.wd_to_path[qid]))
                        masks = event.masks.split("|")
                        if "IN_IGNORED" in masks or \
                           "IN_MOVE_FROM" in masks or \
                           "IN_DELETE" in masks or \
                           "IN_MOVE_SELF" in masks:
                            # path/file does not exist anymore
                            #     (moved/deleted)
                            if event.name is not None:
                                npath = os.path.join(
                                    self.wd_to_path[qid], event.name)
                                get_logger().debug(
                                    "Remove path/file %s" % npath)
                                if self.dslist_fullname == npath and \
                                   not os.path.isfile(self.dslist_fullname) \
                                   and os.path.isdir(self.__path):
                                    subdirs = [
                                        it.path
                                        for it in os.scandir(self.__path)
                                        if it.is_dir()]
                                    get_logger().debug(
                                        "Sub-directories: %s" % str(subdirs))
                                    self._lunch_scandir_watcher(subdirs)
                                    get_logger().debug(
                                        "watcher for subdirectories lunched")

                        elif "IN_ISDIR" not in masks and (
                                "IN_CREATE" in masks or "IN_MOVE_TO" in masks):
                            fn = os.path.join(
                                self.wd_to_path[qid], event.name)
                            dw = None
                            with self.dataset_lock:
                                if fn not in self.dataset_watchers.keys() \
                                   and fn == self.dslist_fullname:
                                    ifn = \
                                        fn[:-(len(self.dslist_filename))] \
                                        + self.idslist_filename
                                    dw = self.dataset_watchers[fn] = \
                                        DatasetWatcher(
                                            self.__config, self.__path,
                                            fn, ifn,
                                            self.__meta, self.__bpath)
                            if dw is not None:
                                dw.start()
                                get_logger().info(
                                    'ScanDirWatcher: Creating '
                                    'DatasetWatcher %s' % fn)
                            dds = []
                            with self.dataset_lock:
                                for path, fn in \
                                        list(self.scandir_watchers.keys()):
                                    ds = self.scandir_watchers.pop((path, fn))
                                    get_logger().info(
                                        'ScanDirWatcher: '
                                        'Stopping ScanDirWatcher %s' % (fn))
                                    ds.running = False
                                    dds.append(ds)
                            while len(dds):
                                ds = dds.pop()
                                ds.join()

                        elif "IN_ISDIR" in masks and (
                                "IN_CREATE" in masks
                                or "IN_MOVE_TO" in masks):
                            if not os.path.isfile(self.dslist_fullname):
                                npath = os.path.join(
                                    self.wd_to_path[qid], event.name)
                                self._lunch_scandir_watcher([npath])
                        # elif "IN_DELETE_SELF" in masks:
                        #     "remove scandir watcher"
                        #     # self.wd_to_path[qid]

                # time.sleep(self.timeout)
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
        with self.dataset_lock:
            for fn, scw in self.dataset_watchers.items():
                get_logger().info(
                    'ScanDirWatcher: Stopping DatasetWatcher %s' % (fn))
                scw.running = False
                scw.join()
        # self.dataset_watchers = []

        with self.scandir_lock:
            for pf, dsw in self.scandir_watchers.items():
                path, fn = pf
                get_logger().info('ScanDirWatcher: '
                                  'Stopping ScanDirWatcher %s' % (fn))
                dsw.running = False
                dsw.join()
        # self.scandir_watchers = []
