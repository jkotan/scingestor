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
from scingestor import safeINotifier

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
class ScanDirWatcherTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None
        self.notifier = safeINotifier.SafeINotifier()

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

    def test_scandir_exist(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        os.mkdir(fdirname)
        os.mkdir(fsubdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)

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
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt1}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher {basedir}'
                    ' {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt2}: {basedir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher {subdir}'
                    ' {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt3}: {subdir}\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: Stopping ScanDirWatcher {btmeta}'
                    '\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt2}: {basedir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt3}: {subdir}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                            subdir=fsubdirname), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_scandir_add(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"'.format(basedir=fdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r6 -l debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r6 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.mkdir(fsubdirname)
            time.sleep(1)
            shutil.rmtree(fsubdirname)

        try:
            for cmd in commands:
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                nodebug = "\n".join([ee for ee in er.split("\n")
                                     if "DEBUG :" not in ee])
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        # 'INFO : BeamtimeWatcher: Removing watch on a '
                        # 'CM event 1: {basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                subdir=fsubdirname), nodebug)
                except Exception:
                    print(er)
                    raise

                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_scandir_exist_basedir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        bdirname = os.path.abspath(dirname)
        os.mkdir(bdirname)
        fdirname = os.path.join(bdirname, "99001234")
        os.mkdir(fdirname)

        fsubdirname = os.path.abspath(os.path.join(fdirname, "raw"))
        os.mkdir(fsubdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)

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
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt1}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher {basedir}'
                    ' {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt2}: {basedir}\n'
                    'INFO : ScanDirWatcher: Create ScanDirWatcher {subdir}'
                    ' {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt3}: {subdir}\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: Stopping ScanDirWatcher {btmeta}'
                    '\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt2}: {basedir}\n'
                    'INFO : ScanDirWatcher: Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt3}: {subdir}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                            subdir=fsubdirname), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(bdirname):
                shutil.rmtree(bdirname)

    def test_scandir_add_basedir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        bdirname = os.path.abspath(dirname)
        os.mkdir(bdirname)
        fdirname = os.path.join(bdirname, "99001234")
        os.mkdir(fdirname)
        fsubdirname = os.path.abspath(os.path.join(fdirname, "raw"))
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"'.format(basedir=fdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r6 -l debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r6 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.mkdir(fsubdirname)
            time.sleep(1)
            shutil.rmtree(fsubdirname)

        try:
            for cmd in commands:
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                nodebug = "\n".join([ee for ee in er.split("\n")
                                     if "DEBUG :" not in ee])
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        # 'INFO : BeamtimeWatcher: Removing watch on a '
                        # 'CM event 1: {basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                subdir=fsubdirname), nodebug)
                except Exception:
                    print(er)
                    raise

                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(bdirname):
                shutil.rmtree(bdirname)


if __name__ == '__main__':
    unittest.main()
