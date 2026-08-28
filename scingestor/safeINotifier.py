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
import time
import threading
import inotify_simple
import queue
import os
# import glob
# import json
from .logger import get_logger


class SafeINotifier(threading.Thread):
    """ singleton wrapper for inotify
    """

    #: (:class:`SafeINotifier`) singleton notifier instance
    _notifier = None
    #: (:class:`threading.Lock`) singleton lock
    _lock = threading.Lock()
    #: (:obj:`bool`) make notifier to be a daemon
    daemon = True

    def __new__(cls, *args, **kwargs):
        """ create a new object if it is the first one
        """
        with cls._lock:
            if not cls._notifier or not cls._notifier.running:
                cls._notifier = super(SafeINotifier, cls).__new__(cls)
                cls._notifier.init()

        return cls._notifier

    def init(self):
        """ constructor

        """
        threading.Thread.__init__(self)

        #: (:obj:`bool`) running loop flag
        self.running = True

        #: (:obj:`int`) watch description queue counter
        self.id_queue_counter = 0
        #: (:obj:`float`) timeout value for inotify get events
        self.inotify_timeout = 1.0

        #: (:class:`inotify_simple.INotify`) notifier object
        self.__notifier = None
        #: (:obj:`int`) notifier id
        self.__notifierid = None
        #: (:obj:`dict` <:obj:`int`, :obj:`queue.Queue`>)
        #: watch description queues
        self.__id_queue = {}
        #: (:obj:`dict` <:obj:`int`, :obj:`int`>)  queue ids watch description
        self.__qid_wd = {}
        #: (:class:`threading.Lock`) watch dictionary lock
        self.__id_queue_lock = threading.Lock()

        #: (:obj:`list` < (:obj:`int`, :obj:`path`, :obj:`int`) >)
        #:  watch description to add i.e. (id, path, masks)
        self.__wd_to_add = []
        #: (:obj:`list` < :obj:`int`>)
        #: queue id of watch description to remove
        self.__wd_to_rm = []

        # start the thread
        self.start()

    def add_watch(self, path, masks):
        """ add watch to notifier

        :param path: watch path
        :type path: :obj:`str`
        :param mask: watch mask
        :type mask: :obj:`int`
        :returns: queue providing events and its id
        :rtype: [:class:`queue.Queue`, :obj:`int`]
        """

        wqueue = queue.Queue()
        with self.__id_queue_lock:
            self.id_queue_counter += 1
            qid = self.id_queue_counter
            self.__id_queue[qid] = wqueue
            get_logger().debug(
                "ADD WATCH: %s %s %s" % (qid, path, masks))
            self.__wd_to_add.append((qid, path, masks))
        return [wqueue, qid]

    def rm_watch(self, qid):
        """ remove watch from notifier

        :param qid: queue id
        :type qid: :obj:`int`
        """
        with self.__id_queue_lock:
            get_logger().debug(
                "REMOVE WATCH: %s" % (qid))
            self.__wd_to_rm.append(qid)
            self.__id_queue.pop(qid)

    def _append(self):
        """ append waches
        """
        for qid, path, masks in self.__wd_to_add:
            try:
                wd = self.__notifier.add_watch(path, masks)
                self.__qid_wd[qid] = wd

            except Exception as e:
                get_logger().warning(
                    'SafeINotifier: append  %s: %s' % (path, str(e)))
        self.__wd_to_add = []

    def _remove(self):
        """ remove waches
        """
        for qid in self.__wd_to_rm:
            if qid in self.__qid_wd:
                wd = self.__qid_wd.pop(qid)
                if wd not in self.__qid_wd.values():
                    try:
                        self.__notifier.rm_watch(wd)
                    except Exception as e:
                        get_logger().debug(
                            'SafeINotifier: remove %s' % str(e))
        self.__wd_to_rm = []

    def run(self):
        """ scandir watcher thread
        """
        self.__notifier = inotify_simple.INotify()
        self.__notifierid = self.__notifier.fileno()

        try:
            while self.running:

                with self.__id_queue_lock:
                    self._append()
                    qlen = len(self.__id_queue)

                if not qlen:
                    time.sleep(self.inotify_timeout)
                else:
                    events = self.__notifier.read(self.inotify_timeout)
                    with self.__id_queue_lock:
                        self._remove()
                        self._append()
                    get_logger().debug('Sc Talk')
                    for event in events:
                        wd = event.wd
                        with self.__id_queue_lock:
                            get_logger().debug(
                                'SN: %s %s %s %s' % (
                                    event.name,
                                    event.mask,
                                    event.wd,
                                    self.__qid_wd
                                ))
                            # get_logger().info(
                            #     'SN: %s %s %s %s' % (
                            #         event.name,
                            #         event.mask,
                            #         event.wd,
                            #         self.__qid_wd
                            #     ))

                            for qid, wd in self.__qid_wd.items():
                                if event.wd == wd and \
                                   qid in self.__id_queue.keys():
                                    wqueue = self.__id_queue[qid]
                                    wqueue.put(event)
                                    get_logger().debug(
                                        'PUT EVENT: %s %s %s %s' % (
                                            event.name,
                                            event.mask,
                                            event.wd, qid
                                        ))

                with self.__id_queue_lock:
                    self._remove()
        finally:
            for wd in self.__qid_wd.values():
                try:
                    self.__notifier.rm_watch(wd)
                except Exception as e:
                    get_logger().debug(
                        'SafeINotifier: finally %s' % str(e))
            if self.__notifierid:
                try:
                    os.close(self.__notifierid)
                    self.__notifierid = None
                except OSError:
                    pass

    def stop(self):
        """ stop the watcher
        """
        self.running = False
