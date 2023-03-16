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

# from .logger import get_logger

import os


class PathConverter:
    """ Path Converter
    """
    def __init__(self, corepath, blpath, usecorepath=False):
        """ constructor

        :param corepath: core path
        :type corepath: :obj:`str`
        :param blpath: beamline path
        :type blpath: :obj:`str`
        :param usecorepath: enabled flag
        :type usecorepath: :obj:`bool`
        """
        #: (:obj:`bool`) use core path
        self.__usecorepath = usecorepath

        #: (:obj:`str`) core path
        self.__corepath = os.path.abspath(corepath)
        #: (:obj:`str`) beamtime path
        self.__bpath = os.path.abspath(blpath)

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) core notify path dict
        self.__core_notify_path = {}
        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) notify core path dict
        self.__notify_core_path = {}

    def to_core(self, path):
        """ converts notify path to core path

        :param path: notify path
        :type path: :obj:`str`
        :returns: core path
        :rtype: :obj:`str`
        """
        if not self.__usecorepath:
            return path
        if path in self.__notify_core_path.keys():
            return self.__notify_core_path[path]
        if path.startswith(self.__bpath):
            cpath = self.__corepath + path[len(self.__bpath):]
            self.__core_notify_path[cpath] = path
            self.__notify_core_path[path] = cpath
            path = cpath
        return path

    def from_core(self, path):
        """ converts core path to notify path

        :param path: core path
        :type path: :obj:`str`
        :returns: notify path
        :rtype: :obj:`str`
        """
        if not self.__usecorepath:
            return path
        if path in self.__core_notify_path.keys():
            return self.__core_notify_path[path]
        if path.startswith(self.__corepath):
            bpath = self.__bpath + path[len(self.__corepath):]
            self.__notify_core_path[bpath] = path
            self.__core_notify_path[path] = bpath
            path = bpath
        return path
