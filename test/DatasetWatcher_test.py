#!/usr/bin/env python
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
# Authors:
#     Jan Kotanski <jan.kotanski@desy.de>
#

import unittest
import os
import sys
import threading
import shutil
# import time

from scingestor import beamtimeWatcher

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


def myinput(w, text):
    myio = os.fdopen(w, 'w')
    myio.write(text)
    myio.close()


class mytty(object):

    def __init__(self, underlying):
        #        underlying.encoding = 'cp437'
        self.__underlying = underlying

    def __getattr__(self, name):
        return getattr(self.__underlying, name)

    def isatty(self):
        return True

    def __del__(self):
        self.__underlying.close()


# test fixture
class DatasetWatcherTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.helperror = "Error: too few arguments\n"

        self.helpinfo = """usage: scicat_dataset_ingestor [-h] [-c CONFIG] [-r RUNTIME] [-l LOG]

BeamtimeWatcher service SciCat Dataset ingestion

optional arguments:
  -h, --help         show this help message and exit
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -r RUNTIME, --runtime RUNTIME
                        stop program after runtime in seconds
  -l LOG, --log LOG  logging level, i.e. debug, info, warning, error, critical

 examples:
       scicat_dataset_ingestor -l debug"""

        self.maxDiff = None

    def runtest(self, argv, pipeinput=None):
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = mystdout = StringIO()
        sys.stderr = mystderr = StringIO()
        old_argv = sys.argv
        sys.argv = argv

        if pipeinput is not None:
            r, w = os.pipe()
            new_stdin = mytty(os.fdopen(r, 'r'))
            old_stdin, sys.stdin = sys.stdin, new_stdin
            tm = threading.Timer(1., myinput, [w, pipeinput])
            tm.start()
        else:
            old_stdin = sys.stdin
            sys.stdin = StringIO()

        etxt = None
        try:
            beamtimeWatcher.main()
        except Exception as e:
            etxt = str(e)
        except SystemExit as e:
            etxt = str(e)
        sys.argv = old_argv

        sys.stdout = old_stdout
        sys.stderr = old_stderr
        sys.stdin = old_stdin
        sys.argv = old_argv
        vl = mystdout.getvalue()
        er = mystderr.getvalue()
        # print(vl)
        # print(er)
        if etxt:
            # print(etxt)
            pass
        # self.assertEqual(etxt, None)
        return vl, er

    def runtestexcept(self, argv, exception):
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_stdin = sys.stdin
        sys.stdin = StringIO()
        sys.stdout = mystdout = StringIO()
        sys.stderr = mystderr = StringIO()

        old_argv = sys.argv
        sys.argv = argv
        try:
            error = False
            beamtimeWatcher.main()
        except exception as e:
            etxt = str(e)
            error = True
        self.assertEqual(error, True)

        sys.argv = old_argv

        sys.stdout = old_stdout
        sys.stderr = old_stderr
        sys.stdin = old_stdin
        sys.argv = old_argv
        vl = mystdout.getvalue()
        er = mystderr.getvalue()
        return vl, er, etxt

    def test_datasetfile_exist(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        os.mkdir(fdirname)
        os.mkdir(fsubdirname)
        os.mkdir(fsubdirname2)
        btmeta = "beamtime-metadata-99001234.json"
        dslist = "scicat-datasets-99001234.lst"
        idslist = "scicat-ingested-datasets-99001234.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        shutil.copy(source, fdirname)
        shutil.copy(lsource, fsubdirname2)
        shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"'.format(basedir=fdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r3'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r3'
                     % cfgfname).split()]
        try:
            for cmd in commands:
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Starting 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Starting ScanDir 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Starting ScanDir 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir2} {btmeta}\n'
                    'INFO : ScanDirWatcher: Starting ScanDir 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Starting {dslist}\n'
                    'INFO : DatasetWatcher: Starting Dataset: '
                    '{dslist} {idslist}\n'
                    'INFO : DatasetWatcher: Scans waiting: '
                    '[\'{sc1}\', \'{sc2}\']\n'
                    'INFO : DatasetWatcher: Scans ingested: []\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc1}\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc2}\n'
                    'INFO : BeamtimeWatcher: Stopping notifier 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: Stopping {btmeta}\n'
                    'INFO : ScanDirWatcher: Stopping notifier 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Stopping {btmeta}\n'
                    'INFO : ScanDirWatcher: Stopping notifier 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Stopping {btmeta}\n'
                    'INFO : ScanDirWatcher: Stopping notifier 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Stopping {dslist}\n'
                    'INFO : ScanDirWatcher: Stopping notifier 1: {dslist}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            subdir=fsubdirname, subdir2=fsubdirname2,
                            dslist=fdslist, idslist=fidslist,
                            sc1='myscan_00001', sc2='myscan_00002'), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
