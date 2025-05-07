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
from dateutil import parser as duparser


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
class BeamtimeWatcherTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.helperror = "Error: too few arguments\n"

        self.helpshort = """usage: scicat_dataset_ingestor [-h]""" \
            """[-c CONFIG] [-r RUNTIME] [-l LOG]
                               [-f LOGFILE] [-t]
scicat_dataset_ingestor: error: unrecognized arguments: """

        self.helpinfo = """usage: scicat_dataset_ingestor [-h]""" \
            """[-c CONFIG] [-r RUNTIME] [-l LOG]
                               [-f LOGFILE] [-t]

BeamtimeWatcher service SciCat Dataset ingestior

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --configuration CONFIG
                        configuration file name
  -r RUNTIME, --runtime RUNTIME
                        stop program after runtime in seconds
  -l LOG, --log LOG     logging level, i.e. """ \
      """debug, info, warning, error, critical
  -f LOGFILE, --log-file LOGFILE
                        log file name
  -t, --timestamps      timestamps in logs

 examples:
      scicat_dataset_ingestor -c ~/.scingestor.yaml
       scicat_dataset_ingestor -c ~/.scingestor.yaml -l debug
"""

        self.maxDiff = None
        self.notifier = safeINotifier.SafeINotifier()

    def runtest(self, argv, pipeinput=None, interrupt=0):
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
            beamtimeWatcher.main(interrupt)
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

    def test_wrong_args(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        helps = ['--wrong', '+asd']
        for hl in helps:
            vl, er, et = self.runtestexcept(
                ['scicat_dataset_ingestor', hl], SystemExit)
            self.assertEqual('', vl)
            self.assertEqual(
                "".join(self.helpshort.split() + [hl]),
                "".join(er.split()))

    def test_noconfig(self):
        # fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))

        vl, er, et = self.runtestexcept(
            ['scicat_dataset_ingestor'], SystemExit)
        self.assertTrue(
            er.endswith(
                'WARNING : BeamtimeWatcher: '
                'Beamtime directories not defined\n'))
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
                self.assertTrue(
                    er.endswith(
                        'WARNING : BeamtimeWatcher: '
                        'Beamtime directories not defined\n'))
                self.assertEqual('', vl)
        finally:
            if os.path.isfile(cfgfname):
                os.remove(cfgfname)

    def test_datasetfile_exist_wrong_btmeta(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        btmeta = "beamtime-metadata-99999999.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = '{"jwt":"12342345"}'
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'scicat_users_login_path: "Users/login"\n' \
            'scicat_datasets_path: "Datasets"\n' \
            'scicat_proposals_path: "Proposals"\n' \
            'scicat_datablocks_path: "OrigDatablocks"\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        logfname = "%s_%s.log" % (self.__class__.__name__, fun)
        logfname1 = "%s_%s.log.1" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -f %s -r3 '
                     % (cfgfname, logfname)).split(),
                    ('scicat_dataset_ingestor --config %s --log-file %s '
                     ' -r3  '
                     % (cfgfname, logfname)).split()]
        # commands.pop()
        lastlog = None
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                with open(logfname) as lfl:
                    er = lfl.read()

                if lastlog:
                    with open(logfname1) as lfl:
                        er2 = lfl.read()
                    self.assertEqual(er2, lastlog)
                lastlog = er

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'WARNING : {btmeta} cannot be watched: '
                        'Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'INFO : BeamtimeWatcher: Removing watch 1: {basedir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                cnt1=cnt),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual("", vl)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(logfname):
                os.remove(logfname)
            if os.path.exists(logfname1):
                os.remove(logfname1)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_wrong_bdir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        btmeta = "beamtime-metadata-99999999.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "\\0"\n' \
            'scicat_url: "{url}"\n' \
            'scicat_users_login_path: "Users/login"\n' \
            'scicat_datasets_path: "Datasets"\n' \
            'scicat_proposals_path: "Proposals"\n' \
            'scicat_datablocks_path: "OrigDatablocks"\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                url=url, vardir=vardir, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        logfname = "%s_%s.log" % (self.__class__.__name__, fun)
        logfname1 = "%s_%s.log.1" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -f %s -r3 '
                     % (cfgfname, logfname)).split(),
                    ('scicat_dataset_ingestor --config %s --log-file %s '
                     ' -r3  '
                     % (cfgfname, logfname)).split()]
        # commands.pop()
        lastlog = None
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                shutil.copy(source, fdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                with open(logfname) as lfl:
                    er = lfl.read()

                if lastlog:
                    with open(logfname1) as lfl:
                        er2 = lfl.read()
                    self.assertEqual(
                        er2.replace("\0", " ").replace(
                            'WARNING : embedded null byte\n', ""),
                        lastlog.replace("\0", " ").replace(
                            'WARNING : embedded null byte\n', ""))
                lastlog = er

                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # print(vl)
                # print(er)

                # nodebug = "\n".join([ee for ee in er.split("\n")
                #                      if (("DEBUG :" not in ee) and
                #                          (not ee.startswith("127.0.0.1")))])
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: \0\n'
                        'WARNING : SafeINotifier: append  '
                        '\0:\x00embedded null character\n'
                        'INFO : BeamtimeWatcher: Removing watch 1: \0\n'
                        .format(cnt1=cnt).replace("\0", " "),
                        '\n'.join(dseri).replace("\0", " ").replace(
                            'WARNING : embedded null byte\n', ""))
                except Exception:
                    print(er)
                    raise
                self.assertEqual("", vl)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(logfname):
                os.remove(logfname)
            if os.path.exists(logfname1):
                os.remove(logfname1)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

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
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt}: '
                    '{basedir}\n'.format(basedir=fdirname, cnt=cnt), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_basedir_timestamps(self):
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
        commands = [('scicat_dataset_ingestor --timestamps -c %s -r3'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor -t --config %s -r3'
                     % cfgfname).split()]
        try:
            for cmd in commands:
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                vl, er = self.runtest(cmd)
                erl = er.split("\n")
                self.assertEqual(len(erl), 3)
                self.assertEqual(len(erl[2]), 0)

                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt}: {basedir}'
                    .format(basedir=fdirname, cnt=cnt),
                    erl[0].split(" ", 3)[-1])
                self.assertEqual(erl[0].split(" ", 3)[2], ":")
                tst = duparser.parse(
                    " ".join(erl[0].split(" ", 3)[:2])).timestamp()
                ct = time.time()
                self.assertTrue(tst <= ct)
                self.assertTrue(ct - tst < 10)

                self.assertEqual(
                    'INFO : BeamtimeWatcher: Removing watch {cnt}: '
                    '{basedir}'.format(basedir=fdirname, cnt=cnt),
                    erl[1].split(" ", 3)[-1])
                self.assertEqual(erl[1].split(" ", 3)[2], ":")
                tst = duparser.parse(
                    " ".join(erl[1].split(" ", 3)[:2])).timestamp()
                ct = time.time()
                self.assertTrue(tst <= ct)
                self.assertTrue(ct - tst < 10)

                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
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
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt2}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt2}: {basedir}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1)), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_black_scandir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scandir_blacklist:\n' \
            '  - "{basedir}"' \
            .format(basedir=fdirname)

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
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    .format(basedir=fdirname,
                            cnt1=cnt), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_wrong_config(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"' \
            'scandir_blacklist:\n' \
            '  - "{basedir}"' \
            .format(basedir=fdirname)

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
                vl, er = self.runtest(cmd)
                self.assertTrue(er.startswith(
                    "WARNING : while parsing a block mapping"))
                self.assertEqual(
                    "WARNING : BeamtimeWatcher: "
                    "Beamtime directories not defined",
                    er.strip().split("\n")[-1])
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_interrupt(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
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
                vl, er = self.runtest(cmd, interrupt=1)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt1}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt2}: {basedir}\n'
                    'WARNING : Keyboard interrupt (SIGINT) received...\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt2}: {basedir}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1)), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_signal(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
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
                vl, er = self.runtest(cmd, interrupt=2)
                self.assertEqual(
                    'INFO : BeamtimeWatcher: Adding watch {cnt1}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt2}: {basedir}\n'
                    'WARNING : SIGTERM received...\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt2}: {basedir}\n'
                    .format(basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1)), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_add(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "bt-mt-99001234.jsn"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtime_filename_prefix: "bt-mt-"\n' \
            'beamtime_filename_postfix: ".jsn"\n'.format(basedir=fdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r4 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r4 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.remove(fullbtmeta)
            time.sleep(1)
            shutil.copy(source, fdirname)

        try:
            # commands.pop()
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
                        # 'IMDM event 1: {basedir}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Adding watch {cnt3}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt3}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{basedir}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3)), nodebug)
                except Exception:
                    print(er)
                    raise
                #  print(er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_black(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        blist = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             "config",
                             "beamtimeid_blacklist.lst")
        shutil.copy(source, fdirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtimeid_blacklist_file: "{blist}"'.format(
                basedir=fdirname, blist=blist)

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
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    .format(basedir=fdirname,
                            cnt1=cnt), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_blacktype(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtime_type_blacklist:\n' \
            '  - "I"\n'.format(
                basedir=fdirname)

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
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    .format(basedir=fdirname,
                            cnt1=cnt), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_add_black(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "bt-mt-99001234.jsn"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        blist = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             "config",
                             "beamtimeid_blacklist.lst")
        fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtimeid_blacklist_file: "{blist}"\n' \
            'beamtime_filename_prefix: "bt-mt-"\n' \
            'beamtime_filename_postfix: ".jsn"\n'.format(
                basedir=fdirname, blist=blist)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r4 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r4 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.remove(fullbtmeta)
            time.sleep(1)
            shutil.copy(source, fdirname)

        try:
            # commands.pop()
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
                        'INFO : BeamtimeWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        .format(basedir=fdirname,
                                cnt1=cnt, cnt2=(cnt + 1)), nodebug)
                except Exception:
                    print(er)
                    raise
                #  print(er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_priv(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001233.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)
        # fullbtmeta = os.path.join(fdirname, btmeta)

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
                    'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                    '{basedir}\n'
                    .format(basedir=fdirname,
                            cnt1=cnt), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_add_priv(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        os.mkdir(fdirname)
        btmeta = "bt-mt-99001233.jsn"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'beamtime_filename_prefix: "bt-mt-"\n' \
            'beamtime_filename_postfix: ".jsn"\n'.format(basedir=fdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r4 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r4 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.remove(fullbtmeta)
            time.sleep(1)
            shutil.copy(source, fdirname)

        try:
            # commands.pop()
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
                        'INFO : BeamtimeWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        .format(basedir=fdirname,
                                cnt1=cnt, cnt2=(cnt + 1)), nodebug)
                except Exception:
                    print(er)
                    raise
                #  print(er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_config_beamtime_metadata_exist_basedir(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        bdirname = os.path.abspath(dirname)
        os.mkdir(bdirname)
        fdirname = os.path.join(bdirname, "99001234")
        os.mkdir(fdirname)

        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        shutil.copy(source, fdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)

        bdirname, _ = os.path.split(fdirname)
        cfg = 'beamtime_base_dir: "{basedir}"'.format(basedir=bdirname)

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
                    'INFO : BeamtimeWatcher: Adding base watch {cnt1}: '
                    '{root}\n'
                    'INFO : BeamtimeWatcher: Adding watch {cnt2}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                    '{basedir} {btmeta}\n'
                    'INFO : ScanDirWatcher: Adding watch {cnt3}: {basedir}\n'
                    'INFO : BeamtimeWatcher: Removing watch {cnt2}: '
                    '{basedir}\n'
                    'INFO : BeamtimeWatcher: Removing base watch {cnt1}: '
                    '{root}\n'
                    'INFO : BeamtimeWatcher: '
                    'Stopping ScanDirWatcher {btmeta}\n'
                    'INFO : ScanDirWatcher: Removing watch {cnt3}: {basedir}\n'
                    .format(root=bdirname, basedir=fdirname, btmeta=fullbtmeta,
                            cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2)), er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(bdirname):
                shutil.rmtree(bdirname)

    def test_config_beamtime_metadata_exist_basedir_add(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        bdirname = os.path.abspath(dirname)
        os.mkdir(bdirname)
        fdirname = os.path.join(bdirname, "99001234")
        os.mkdir(fdirname)
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        bdirname, _ = os.path.split(fdirname)
        cfg = 'beamtime_base_dir: "{basedir}"'.format(basedir=bdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r12 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r12 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(source, fdirname)
            time.sleep(3)
            os.remove(fullbtmeta)
            time.sleep(3)
            shutil.copy(source, fdirname)

        try:
            # commands.pop()
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
                        'INFO : BeamtimeWatcher: Adding base watch {cnt1}: '
                        '{root}\n'
                        'INFO : BeamtimeWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{basedir}\n'
                        # 'INFO : BeamtimeWatcher: Removing watch on a '
                        # 'IMDM event 1: {basedir}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Adding watch {cnt4}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt4}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing base watch {cnt1}: '
                        '{root}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{basedir}\n'
                        .format(root=bdirname, basedir=fdirname,
                                btmeta=fullbtmeta,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4)), nodebug)
                except Exception:
                    print(er)
                    raise
                #  print(er)
                self.assertEqual('', vl)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(bdirname):
                shutil.rmtree(bdirname)

    def test_config_beamtime_metadata_basedir_add(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        bdirname = os.path.abspath(dirname)
        os.mkdir(bdirname)
        fdirname = os.path.join(bdirname, "99001234")
        btmeta = "beamtime-metadata-99001234.json"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        fullbtmeta = os.path.join(fdirname, btmeta)

        bdirname, _ = os.path.split(fdirname)
        cfg = 'beamtime_base_dir: "{basedir}"'.format(basedir=bdirname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r5 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r5 -l debug'
                     % cfgfname).split()]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(1)
            os.mkdir(fdirname)
            time.sleep(1)
            shutil.copy(source, fdirname)
            time.sleep(1)
            os.remove(fullbtmeta)
            time.sleep(1)
            shutil.copy(source, fdirname)

        try:
            # commands.pop()
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
                        'INFO : BeamtimeWatcher: Adding base watch {cnt1}: '
                        '{root}\n'
                        'INFO : BeamtimeWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{basedir}\n'
                        # 'INFO : BeamtimeWatcher: Removing watch on a '
                        # 'IMDM event 1: {basedir}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Adding watch {cnt4}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt5}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt4}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Removing base watch {cnt1}: '
                        '{root}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{basedir}\n'
                        .format(root=bdirname, basedir=fdirname,
                                btmeta=fullbtmeta,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4)), nodebug)
                except Exception:
                    print(er)
                    raise
                #  print(er)
                self.assertEqual('', vl)
                if os.path.isdir(fdirname):
                    shutil.rmtree(fdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(bdirname):
                shutil.rmtree(bdirname)


if __name__ == '__main__':
    unittest.main()
