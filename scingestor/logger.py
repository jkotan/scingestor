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

import logging
import logging.handlers
import datetime
import os


levels = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

_logger = None


class AccSecFormatter(logging.Formatter):
    """ micro-second formatter
    """

    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        currentdatatime = self.converter(record.created)
        strftime = currentdatatime.strftime("%Y-%m-%d %H:%M:%S")
        mstrftime = "%s.%04d" % (strftime, record.msecs * 10)
        return mstrftime


def init_logger(name=__name__, level='debug', timestamps=False, logfile=None):
    """ init logger

    :param name: logger name
    :type name: :obj:`str`
    :param level: logging level
    :type level: :obj:`str`
    :param timestamps: timestamps flag
    :type timestamps: :obj:`bool`
    :param logfile: logger file name
    :type logfile: :obj:`str`
    """

    global _logger
    _logger = logging.getLogger()

    rollover = False
    if logfile:
        rollover = os.path.isfile(logfile)

    ll = levels.get(level, "debug")
    _logger.setLevel(ll)
    if logfile:
        handler = logging.handlers.RotatingFileHandler(
            logfile, backupCount=5)
    else:
        handler = logging.StreamHandler()
    handler.setLevel(ll)
    fmt = logging.Formatter('%(levelname)s : %(message)s')
    if timestamps:
        fmt = AccSecFormatter("%(asctime)s : %(levelname)s : %(message)s")
    else:
        fmt = logging.Formatter('%(levelname)s : %(message)s')
    handler.setFormatter(fmt)
    _logger.addHandler(handler)
    if logfile and rollover:
        for h in _logger.handlers:
            if isinstance(h, logging.handlers.BaseRotatingHandler):
                h.doRollover()


def get_logger():
    """ provides logger object

    :rtype: :class:`logging.logger`
    :returns: logger object
    """
    global _logger
    return _logger
