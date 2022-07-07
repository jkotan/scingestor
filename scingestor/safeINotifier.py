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
import inotifyx
import queue
# import glob
# import json
# import os
from .logger import get_logger


class EventData:
    """ event data """

    def __init__(self, name, wd, masks):
        """ constructor

        :param name: name
        :type name: :obj:`str`
        :param wd: watch descriptor id
        :type wd: :obj:`int`
        :param masks: mask description
        :type maks: :obj:`str`
        """
        # (:obj:`str`) name
        self.name = name
        # (:obj:`int`) wd id
        self.wd = wd
        # (:obj:`str`) mask
        self.masks = masks


class SafeINotifier(threading.Thread):
    """ singleton wrapper for inotifyx
    """

    # (:class:`SafeINotifier`) singleton notifier instance
    _notifier = None
    # (:class:`threading.Lock`) singleton lock
    _lock = threading.Lock()

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

        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 0.01
        # (:obj:`bool`) running loop flag
        self.running = True
        # (:obj:`int`) notifier ID
        self.notifier = None
        # (:obj:`dict` <:obj:`int`, :obj:`queue.Queue`>)
        # watch description queues
        self.id_queue = {}
        # (:obj:`dict` <:obj:`int`, :obj:`int`>)  queue ids watch description
        self.qid_wd = {}
        # (:obj:`int`) watch description queue counter
        self.id_queue_counter = 0
        # (:class:`threading.Lock`) watch dictionary lock
        self.id_queue_lock = threading.Lock()

        # (:obj:`list` < (:obj:`int`, :obj:`path`, :obj:`int`) >)
        # watch description to add i.e. (id, path, masks)
        self.wd_to_add = []
        # (:obj:`list` < :obj:`int`>)
        # queue id of watch description to remove
        self.wd_to_rm = []

        # start the thread
        self.start()

    def add_watch(self, path, masks):
        """ add watch to notifier

        :param path: watch path
        :type path: :obj:`str`
        :param mask: watch mask
        :type mask: :obj:`int`
        :returns: queue providing events and its id
        :rtype: [:class:`threading.queue`, :obj:`int`]
        """

        wqueue = queue.Queue()
        with self.id_queue_lock:
            self.id_queue_counter += 1
            qid = self.id_queue_counter
            self.id_queue[qid] = wqueue
            self.wd_to_add.append((qid, path, masks))
        return [wqueue, qid]

    def rm_watch(self, qid):
        """ add watch to notifier

        :param qid: queue id
        :type qid: :obj:`int`
        """
        with self.id_queue_lock:
            self.wd_to_rm.append(qid)
            self.id_queue.pop(qid)

    def run(self):
        """ scandir watcher thread
        """
        self.notifier = inotifyx.init()

        try:
            while self.running:

                with self.id_queue_lock:
                    for qid, path, masks in self.wd_to_add:
                        try:
                            wd = inotifyx.add_watch(self.notifier, path, masks)
                            self.qid_wd[qid] = wd

                        except Exception as e:
                            get_logger().warning(
                                'SafeINotifier: %s: %s' % (path, str(e)))
                    self.wd_to_add = []

                with self.id_queue_lock:
                    for qid in self.wd_to_rm:
                        if qid in self.qid_wd:
                            wd = self.qid_wd.pop(qid)
                            if wd not in self.qid_wd.values():
                                try:
                                    inotifyx.rm_watch(self.notifier, wd)
                                except Exception as e:
                                    get_logger().debug(
                                        'SafeINotifier: %s' % str(e))
                    self.wd_to_rm = []
                    qlen = len(self.id_queue)

                if not qlen:
                    time.sleep(self.timeout)
                else:
                    events = inotifyx.get_events(self.notifier, self.timeout)
                    get_logger().debug('Sc Talk')
                    for event in events:
                        wd = event.wd
                        with self.id_queue_lock:
                            get_logger().debug(
                                'SN: %s %s %s' % (event.name,
                                                  event.get_mask_description(),
                                                  event.wd))
                            for qid, wd in self.qid_wd.items():
                                if qid in self.id_queue.keys():
                                    wqueue = self.id_queue[qid]
                                    wqueue.put(
                                        EventData(
                                            event.name,
                                            wd,
                                            event.get_mask_description()))

        finally:
            for wd in self.qid_wd.values():
                try:
                    inotifyx.rm_watch(self.notifier, wd)
                except Exception as e:
                    get_logger().debug(
                        'SafeINotifier: %s' % str(e))

    def stop(self):
        """ stop the watcher
        """
        self.running = False
