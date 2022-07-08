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

levels = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

_logger = None


def init_logger(name=__name__, level='debug'):
    """ init logger
    """
    global _logger
    _logger = logging.getLogger()
    ll = levels.get(level, "debug")
    _logger.setLevel(ll)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(ll)
    stdout_handler.setFormatter(
        logging.Formatter('%(levelname)s : %(message)s'))
    # logging.Formatter("%(created)s : %(levelname)s : %(message)s",
    #                   "%Y-%m-%d %H:%M:%S"))
    _logger.addHandler(stdout_handler)


def get_logger():
    global _logger
    return _logger
