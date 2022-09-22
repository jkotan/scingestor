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
#
#
import os
import time
import threading
import queue
import inotifyx

from .safeINotifier import SafeINotifier
from .datasetIngestor import DatasetIngestor
from .logger import get_logger


class DatasetWatcher(threading.Thread):
    """ Dataset  Watcher
    """

    def __init__(self, configuration,
                 path, dsfile, idsfile, meta, beamtimefile):
        """ constructor

        :param configuration: dictionary with the ingestor configuration
        :type configuration: :obj:`dict` <:obj:`str`, `any`>
        :param path: scan dir path
        :type path: :obj:`str`
        :param dsfile: file with a dataset list
        :type dsfile: :obj:`str`
        :param dsfile: file with a ingester dataset list
        :type dsfile: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param beamtimefile: beamtime filename
        :type beamtimefile: :obj:`str`
        """
        threading.Thread.__init__(self)

        #: (:obj:`bool`) running loop flag
        self.running = True

        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}
        #: (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        #: (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        #: (:obj:`float`) delay time for ingestion in s
        self.__delay = 5
        #: (:obj:`int`) notifier ID
        self.__notifier = None
        #: (:obj:`dict` <:obj:`int`, :obj:`str`>) watch description paths
        self.__wd_to_path = {}
        #: (:obj:`dict` <:obj:`int`, :obj:`str`>)
        #                               beamtime watch description paths
        self.__wd_to_queue = {}

        #: (:obj:`float`) timeout value for inotifyx get events in s
        self.__timeout = 0.01
        #: (:obj:`float`) max count of recheck the dataset list
        self.__maxcounter = 100

        if "max_request_tries_number" in self.__config.keys():
            try:
                self.__maxcounter = int(
                    self.__config["max_request_tries_number"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "get_event_timeout" in self.__config.keys():
            try:
                self.__timeout = float(self.__config["get_event_timeout"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "ingestion_delay_time" in self.__config.keys():
            try:
                self.__delay = float(self.__config["ingestion_delay_time"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        #: (:class:`scingestor.datasetIngestor.DatasetIngestor`)
        #  dataset ingestor
        self.__ingestor = DatasetIngestor(
            configuration, path, dsfile, idsfile, meta, beamtimefile)

    def _start_notifier(self, path):
        """ start notifier

        :param path: beamtime file subpath
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
                path,
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MODIFY |
                inotifyx.IN_OPEN |
                inotifyx.IN_CLOSE_WRITE | inotifyx.IN_DELETE |
                inotifyx.IN_MOVE_SELF |
                inotifyx.IN_ALL_EVENTS |
                inotifyx.IN_MOVED_TO | inotifyx.IN_MOVED_FROM)
            self.__wd_to_path[watch_descriptor] = path
            self.__wd_to_queue[watch_descriptor] = wqueue
            get_logger().info('DatasetWatcher: Adding watch %s: %s %s' % (
                watch_descriptor, self.__dsfile, self.__idsfile))
        except Exception as e:
            get_logger().warning('%s: %s' % (path, str(e)))

    def _stop_notifier(self):
        """ stop notifier
        """
        for wd in list(self.__wd_to_path.keys()):
            self.__notifier.rm_watch(wd)
            path = self.__wd_to_path.pop(wd, None)
            self.__wd_to_queue.pop(wd, None)
            get_logger().info(
                'ScanDirWatcher: '
                'Removing watch %s: %s' % (str(wd), path))

    def run(self):
        """ scandir watcher thread
        """
        self._start_notifier(self.__dsfile)
        try:
            self.__ingestor.check_list()
        except Exception as e:
            get_logger().warning(str(e))

        get_logger().info(
            'DatasetWatcher: Waiting datasets: %s'
            % str(self.__ingestor.waiting_datasets()))
        get_logger().info(
            'DatasetWatcher: Ingested datasets: %s'
            % str(self.__ingestor.ingested_datasets()))
        if self.__ingestor.waiting_datasets():
            time.sleep(self.__delay)
        if self.__ingestor.waiting_datasets():
            try:
                token = self.__ingestor.get_token()
                for scan in self.__ingestor.waiting_datasets():
                    self.__ingestor.ingest(scan, token)
                self.__ingestor.clear_waiting_datasets()
            except Exception as e:
                get_logger().warning(str(e))

        counter = 0
        try:
            while self.running:

                get_logger().debug('Sc Talk')

                if not self.__wd_to_queue:
                    time.sleep(self.__timeout/10.)
                for qid in list(self.__wd_to_queue.keys()):
                    wqueue = self.__wd_to_queue[qid]
                    try:
                        event = wqueue.get(block=True, timeout=self.__timeout)
                    except queue.Empty:
                        break
                    if qid in self.__wd_to_path.keys():
                        # get_logger().info(
                        #     'Ds: %s %s %s' % (event.name,
                        #                       event.masks,
                        #                       self.__wd_to_path[qid]))
                        get_logger().debug(
                            'Ds: %s %s %s' % (event.name,
                                              event.masks,
                                              self.__wd_to_path[qid]))
                        masks = event.masks.split("|")
                        if "IN_CLOSE_WRITE" in masks:
                            if event.name:
                                fdir, fname = os.path.split(
                                    self.__wd_to_path[qid])
                                ffn = os.path.join(fdir, event.name)
                            else:
                                ffn = self.__wd_to_path[qid]
                            if ffn is not None and ffn == self.__dsfile:
                                get_logger().debug(
                                    'DatasetWatcher: Changed %s' % ffn)
                                try:
                                    self.__ingestor.check_list()
                                except Exception as e:
                                    get_logger().warning(str(e))
                                    continue
                        elif "IN_MODIFY" in masks or "IN_OPEN" in masks:
                            if event.name:
                                fdir, fname = os.path.split(
                                    self.__wd_to_path[qid])
                                ffn = os.path.join(fdir, event.name)
                                if ffn is not None and \
                                   ffn == self.__dsfile:
                                    get_logger().debug(
                                        'DatasetWatcher: Changed %s' % ffn)
                                    try:
                                        self.__ingestor.check_list()
                                    except Exception as e:
                                        get_logger().warning(str(e))
                                        continue

                if counter == self.__maxcounter:
                    # if inotify does not work
                    counter = 0
                    # get_logger().info(
                    #     'DatasetWatcher: Re-check dataset list after %s s'
                    #     % self.__maxcounter)
                    get_logger().debug(
                        'DatasetWatcher: Re-check dataset list after %s s'
                        % self.__maxcounter)
                    try:
                        self.__ingestor.check_list()
                    except Exception as e:
                        get_logger().warning(str(e))
                        continue
                elif self.__maxcounter > counter:
                    get_logger().debug(
                        'DatasetWatcher: increase counter %s/%s ' %
                        (counter, self.__maxcounter))
                    # get_logger().info(
                    #     'DatasetWatcher: increase counter %s/%s ' %
                    #     (counter, self.__maxcounter))
                    counter += 1

                if self.__ingestor.waiting_datasets():
                    time.sleep(self.__delay)
                    try:
                        token = self.__ingestor.get_token()
                    except Exception as e:
                        get_logger().warning(str(e))
                        continue
                    for scan in self.__ingestor.waiting_datasets():
                        try:
                            self.__ingestor.ingest(scan, token)
                        except Exception as e:
                            get_logger().warning(str(e))
                            continue
                    self.__ingestor.clear_waiting_datasets()
                # else:
                #     time.sleep(self.__timeout)
        finally:
            self.stop()

    def stop(self):
        """ stop the watcher
        """
        self.running = False
        time.sleep(0.2)
        self._stop_notifier()
