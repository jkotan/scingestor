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
import time

from scingestor import beamtimeWatcher


try:
    from .SciCatTestServer import SciCatTestServer, SciCatMockHandler
except Exception:
    from SciCatTestServer import SciCatTestServer, SciCatMockHandler


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

        self.maxDiff = None

    def setUp(self):
        self.starthttpserver()

    def starthttpserver(self):
        self.__server = SciCatTestServer(('', 8881), SciCatMockHandler)

        self.__thread = threading.Thread(None, self.__server.run)
        self.__thread.start()

    def stophttpserver(self):
        if self.__server is not None:
            self.__server.shutdown()
        if self.__thread is not None:
            self.__thread.join()

    def tearDown(self):
        self.stophttpserver()

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
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        logdir = "/"
        cred = "12342345"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r8'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r8'
                     % cfgfname).split()]
        try:
            for cmd in commands:
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser  if not ln.startswith("127.0.0.1")]
                sero = [ln for ln in ser  if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir2} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Creating DatasetWatcher {dslist}\n'
                    'INFO : DatasetWatcher: Adding watch: '
                    '{dslist} {idslist}\n'
                    'INFO : DatasetWatcher: Scans waiting: '
                    '[\'{sc1}\', \'{sc2}\']\n'
                    'INFO : DatasetWatcher: Scans ingested: []\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc1}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc1} {subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc2}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc2} {subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : BeamtimeWatcher: Removing watch 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Stopping DatasetWatcher {dslist}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {dslist}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            subdir=fsubdirname, subdir2=fsubdirname2,
                            dslist=fdslist, idslist=fidslist,
                            sc1='myscan_00001', sc2='myscan_00002'),
                    "\n".join(seri))
                self.assertEqual('Login: ingestor\n', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add(self):
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
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        shutil.copy(source, fdirname)
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        logdir = "/"
        cred = "12342345"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'ingestor_log_dir: "{logdir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, logdir=logdir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        commands = [('scicat_dataset_ingestor -c %s -r24'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r24'
                     % cfgfname).split()]

        def test_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        #        commands.pop()
        try:
            for cmd in commands:
                # print(cmd)
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=test_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser  if not ln.startswith("127.0.0.1")]
                sero = [ln for ln in ser  if ln.startswith("127.0.0.1")]
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher '
                    '{subdir2} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Creating DatasetWatcher {dslist}\n'
                    'INFO : DatasetWatcher: Adding watch: '
                    '{dslist} {idslist}\n'
                    'INFO : DatasetWatcher: Scans waiting: '
                    '[\'{sc1}\', \'{sc2}\']\n'
                    'INFO : DatasetWatcher: Scans ingested: []\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc1}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc1} {subdir2}/{sc1}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc2}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc2} {subdir2}/{sc2}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc3}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc3} {subdir2}/{sc3}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                    'INFO : DatasetWatcher: Ingesting: {dslist} {sc4}\n'
                    'INFO : DatasetWatcher: Generating metadata: '
                    '{sc4} {subdir2}/{sc4}.scan.json\n'
                    'INFO : DatasetWatcher: Generating origdatablock metadata:'
                    ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                    'INFO : BeamtimeWatcher: Removing watch 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {basedir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {subdir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {subdir2}\n'
                    'INFO : ScanDirWatcher: Stopping DatasetWatcher {dslist}\n'
                    'INFO : ScanDirWatcher: Removing watch 1: {dslist}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            subdir=fsubdirname, subdir2=fsubdirname2,
                            dslist=fdslist, idslist=fidslist,
                            sc1='myscan_00001', sc2='myscan_00002',
                            sc3='myscan_00003', sc4='myscan_00004'),
                    "\n".join(seri))
                self.assertEqual('Login: ingestor\n'
                                 'Login: ingestor\n', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
