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
class BeamlineWatcherTest(unittest.TestCase):

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

    def test_help(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        helps = ['-h', '--help']
        for hl in helps:
            vl, er, et = self.runtestexcept(
                ['scicat_dataset_ingestor', hl], SystemExit)
            self.assertEqual(
                "".join(self.helpinfo.split()).replace(
                    "optionalarguments:", "options:"),
                "".join(vl.split()).replace("optionalarguments:", "options:"))
            self.assertEqual('', er)

    def test_noconfig(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        vl, er, et = self.runtestexcept(
            ['scicat_dataset_ingestor'], SystemExit)
        self.assertEqual(
            'WARNING : BeamtimeWatcher: Beamtime directories not defined\n',
            er)
        self.assertEqual('', vl)

    def test_config_empty(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        cfg = '\n'
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        try:
            commands = [('scicat_dataset_ingestor -c %s'
                         % cfgfname).split(),
                        ('scicat_dataset_ingestor --config %s'
                         % cfgfname).split()]
            for cmd in commands:
                vl, er, et = self.runtestexcept(
                    cmd, SystemExit)
                self.assertEqual(
                    'WARNING : BeamtimeWatcher: '
                    'Beamtime directories not defined\n',
                    er)
                self.assertEqual('', vl)
        finally:
            if os.path.isdir(cfgfname):
                os.remove(cfgfname)

    def test_config_basedir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)

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
                vl, er = self.runtest(cmd)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Starting 1: {basedir}\n'
                    'INFO : BeamtimeWatcher: Stopping notifier 1: '
                    '{basedir}\n'.format(basedir=fdirname), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
