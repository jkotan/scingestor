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
import json
import uuid
import numpy as np

from scingestor import beamtimeWatcher
from scingestor import safeINotifier

from nxstools import filewriter

try:
    from .SciCatTestServer import SciCatTestServer, SciCatMockHandler
except Exception:
    from SciCatTestServer import SciCatTestServer, SciCatMockHandler


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

WRITERS = {}
try:
    from nxstools import h5pywriter
    WRITERS["h5py"] = h5pywriter
except Exception:
    pass

try:
    from nxstools import h5cppwriter
    WRITERS["h5cpp"] = h5cppwriter
except Exception:
    pass


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
class DatasetWatcherH5Test(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.maxDiff = None
        self.notifier = safeINotifier.SafeINotifier()

        if "h5cpp" in WRITERS.keys():
            self.writer = "h5cpp"
        else:
            self.writer = "h5py"

    def myAssertDict(self, dct, dct2, skip=None, parent=None):
        parent = parent or ""
        self.assertTrue(isinstance(dct, dict))
        self.assertTrue(isinstance(dct2, dict))
        if len(list(dct.keys())) != len(list(dct2.keys())):
            print(list(dct.keys()))
            print(list(dct2.keys()))
        self.assertEqual(
            len(list(dct.keys())), len(list(dct2.keys())))
        for k, v in dct.items():
            if parent:
                node = "%s.%s" % (parent, k)
            else:
                node = k
            if k not in dct2.keys():
                print("%s not in %s" % (k, dct2))
            self.assertTrue(k in dct2.keys())
            if not skip or node not in skip:
                if isinstance(v, dict):
                    self.myAssertDict(v, dct2[k], skip, node)
                else:
                    self.assertEqual(v, dct2[k])

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

    def test_datasetfile_exist_h5(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'add_empty_units: False\n' \
            'log_generator_commands: true\n' \
            'ingest_dataset_attachment: false\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'hidden_attributes: "{hattr}"\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'master_file_extension_list:\n' \
            '  - "nxs"\n' \
            '  - "fio"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
                'sample_id: "H2O/1232"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n',
                'water/21232',
            ],
        ]
        sids = ["H2O/1232", 'water/21232']
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    sdesc = arg[10]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sdes = sample.create_field("description", "string")
                    sdes.write(sdesc)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                copymapfile=cpmapname,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'sampleId': sids[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {},
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument_name': args[0][3],
                      'sample_name': args[0][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'description': {'value': '%s' % args[0][10]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'sampleId': sids[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {},
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument_name': args[1][3],
                      'sample_name': args[1][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'description': {'value': '%s' % args[1][10]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_attachment_nodata(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'log_generator_commands: true\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'add_empty_units: False\n' \
            'hidden_attributes: "{hattr}"\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'master_file_extension_list:\n' \
            '  - "nxs"\n' \
            '  - "fio"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r15 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r15 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
                'sample_id: "H2O/1232"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n',
                'water/21232',
            ],
        ]
        sids = ["H2O/1232", 'water/21232']
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    sdesc = arg[10]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    # fl[0,:] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sdes = sample.create_field("description", "string")
                    sdes.write(sdesc)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                copymapfile=cpmapname,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'sampleId': sids[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [0, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument_name': args[0][3],
                      'sample_name': args[0][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'description': {'value': '%s' % args[0][10]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'sampleId': sids[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [0, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument_name': args[1][3],
                      'sample_name': args[1][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'description': {'value': '%s' % args[1][10]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_attachment_mca(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'attachment_metadata_postfix: ".attachment.json"\n' \
            'attachment_signals_generator_switch: " -s {{signals}} "\n' \
            'attachment_axes_generator_switch: " -e {{axes}} "\n' \
            'attachment_frame_generator_switch: " -m {{frame}} "\n' \
            'log_generator_commands: true\n' \
            'add_empty_units: False\n' \
            'hidden_attributes: "{hattr}"\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'master_file_extension_list:\n' \
            '  - "nxs"\n' \
            '  - "fio"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r15 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r15 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
                'sample_id: "H2O/1232"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n',
                'water/21232',
            ],
        ]
        sids = ["H2O/1232", 'water/21232']
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    sdesc = arg[10]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field("intimage", "uint32",
                                          [1, 30], [1, 30])
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sdes = sample.create_field("description", "string")
                    sdes.write(sdesc)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                copymapfile=cpmapname,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'sampleId': sids[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [1, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument_name': args[0][3],
                      'sample_name': args[0][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'description': {'value': '%s' % args[0][10]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'sampleId': sids[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99991173.99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [1, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument_name': args[1][3],
                      'sample_name': args[1][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'description': {'value': '%s' % args[1][10]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 2)
                self.assertEqual(len(self.__server.attachments[0]), 2)
                self.assertEqual(self.__server.attachments[0][0],
                                 '99001234/myscan_00001')
                self.myAssertDict(
                    json.loads(self.__server.attachments[0][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'datasetId': '99001234/myscan_00001',
                        'caption': '',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[0][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                self.assertEqual(len(self.__server.attachments[1]), 2)
                self.assertEqual(self.__server.attachments[1][0],
                                 '99001234/myscan_00002')
                self.myAssertDict(
                    json.loads(self.__server.attachments[1][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'datasetId': '99001234/myscan_00002',
                        'caption': '',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[1][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_attachment_mca_master_in(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        hattr = "nexdatas_source,nexdatas_strategy,units,NX_class,source," \
            "source_name,source_type,strategy,type"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        copymap = 'scientificMetadata.instrument_name ' \
            'scientificMetadata.instrument.name.value\n' \
            'scientificMetadata.sample_name ' \
            'scientificMetadata.sample.name.value\n' \
            'scientificMetadata.instrument.detector.intimage\n'
        cpmapname = "%s_%s.lst" % (self.__class__.__name__, fun)
        with open(cpmapname, "w+") as cf:
            cf.write(copymap)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'chmod_json_files: "{chmod}"\n' \
            'chmod_generator_switch: " -x {{chmod}} "\n' \
            'attachment_metadata_postfix: ".attachment.json"\n' \
            'attachment_signals_generator_switch: " -s {{signals}} "\n' \
            'attachment_axes_generator_switch: " -e {{axes}} "\n' \
            'attachment_frame_generator_switch: " -m {{frame}} "\n' \
            'log_generator_commands: true\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'add_empty_units: False\n' \
            'hidden_attributes: "{hattr}"\n' \
            'hidden_attributes_generator_switch: ' \
            '" -n {{hiddenattributes}} "\n' \
            'metadata_copy_map_file: "{cpmapfile}"\n' \
            'metadata_copy_map_file_generator_switch: ' \
            '" --copy-map-file {{copymapfile}} "\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'master_file_extension_list:\n' \
            '  - "nxs"\n' \
            '  - "fio"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile, chmod=chmod, hattr=hattr,
                cpmapfile=cpmapname)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r15 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r15 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
                'sample_id: "H2O/1232"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n',
                'water/21232',
            ],
        ]
        sids = ["H2O/1232", 'water/21232']
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    os.mkdir(os.path.join(fsubdirname2, arg[0].split(".")[0]))
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0].split(".")[0], arg[0])
                    nxsfilename2 = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename2[:-4] + ".scan.json"
                    dbfilename = nxsfilename2[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    sdesc = arg[10]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field("intimage", "uint32",
                                          [1, 30], [1, 30])
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sdes = sample.create_field("description", "string")
                    sdes.write(sdesc)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}/{sc1}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}/{sc1}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}/{sc2}.nxs -r raw/special  -x 0o662  '
                        '-n nexdatas_source,nexdatas_strategy,units,NX_class,'
                        'source,source_name,source_type,strategy,type  '
                        '--copy-map-file {copymapfile}  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}/{sc2}.nxs -x 0o662 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                copymapfile=cpmapname,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n",
                    vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'sampleId': sids[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [1, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument_name': args[0][3],
                      'sample_name': args[0][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'description': {'value': '%s' % args[0][10]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'sampleId': sids[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {
                      'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {
                          "lkintimage": {
                              'shape': [1, 30]
                          }
                      },
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument_name': args[1][3],
                      'sample_name': args[1][7],
                      'instrument': {
                          'detector': {},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'description': {'value': '%s' % args[1][10]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt', '99001234-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 2)
                self.assertEqual(len(self.__server.attachments[0]), 2)
                self.assertEqual(self.__server.attachments[0][0],
                                 '99001234/myscan_00001')
                self.myAssertDict(
                    json.loads(self.__server.attachments[0][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'caption': '',
                        'datasetId': '99001234/myscan_00001',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[0][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                self.assertEqual(len(self.__server.attachments[1]), 2)
                self.assertEqual(self.__server.attachments[1][0],
                                 '99001234/myscan_00002')
                self.myAssertDict(
                    json.loads(self.__server.attachments[1][1]),
                    {
                        'ownerGroup': '99001234-dmgt',
                        'caption': '',
                        'datasetId': '99001234/myscan_00002',
                        'thumbnail':
                        "data:image/png;base64,i",
                        'accessGroups': [
                            '99001234-dmgt', '99001234-clbt', '99001234-part',
                            'p00dmgt', 'p00staff'],
                    }, skip=["thumbnail"])
                self.assertTrue(
                    json.loads(
                        self.__server.attachments[1][1])["thumbnail"].
                    startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.exists(cpmapname):
                os.remove(cpmapname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'log_generator_commands: true\n' \
            'ingest_dataset_attachment: false\n' \
            'scicat_proposal_id_pattern: "{{proposalid}}.{{beamtimeid}}"\n' \
            'add_empty_units: False\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99991173.99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {'NX_class': 'NXdata'},
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [0, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5_attachment_counter(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'plot_file_extension_list:\n' \
            '  - "nxs"\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'log_generator_commands: true\n' \
            'add_empty_units: False\n' \
            'attachment_metadata_generator: '\
            '"nxsfileinfo attachment  -w {{ownergroup}} -c {{accessgroups}} ' \
            ' {{scanpath}}/{{scanname}}.{{plotext}} ' \
            '-o {{metapath}}/{{scanname}}{{attachmentpostfix}} "\n' \
            'scicat_attachments_path: "Datasets/{{pid}}/Attachments"\n' \
            'call_metadata_generated_callback: True\n' \
            'metadata_generated_callback: "echo \\" APPEND ' \
            '{{scanpath}}/{{scanname}}.scan.json ' \
            '{{scanpath}}/{{scanname}}.origdatablock.json ' \
            '{{scanpath}}/{{scanname}}.attachment.json\\""\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r40 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r40 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]
                    mot03 = list(range(3, 13))

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    rt.attributes.create(
                        "default", "string").write("entry12345")
                    entry = rt.create_group("entry12345", "NXentry")
                    entry.attributes.create("default", "string").write("data")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    dt.attributes.create(
                        "signal", "string").write("lk_spectrum")
                    dt.attributes.create("axes", "string").write("mot03")
                    mt = dt.create_field("mot03", "uint32", [10], [10])
                    mt.write(mot03)

                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field(
                        "intimage", "uint32", [1, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    filewriter.link(
                        "/entry12345/instrument/detector/spectrum", dt,
                        "lk_spectrum")

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff  {subdir2}/{sc1}.nxs '
                        '-o {subdir2}/{sc1}.attachment.json \n'
                        'INFO : DatasetIngestor: Metadata generated callback: '
                        'echo " APPEND {subdir2}/{sc1}.scan.json '
                        '{subdir2}/{sc1}.origdatablock.json '
                        '{subdir2}/{sc1}.attachment.json" \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff  {subdir2}/{sc2}.nxs '
                        '-o {subdir2}/{sc2}.attachment.json \n'
                        'INFO : DatasetIngestor: Metadata generated callback: '
                        'echo " APPEND {subdir2}/{sc2}.scan.json '
                        '{subdir2}/{sc2}.origdatablock.json '
                        '{subdir2}/{sc2}.attachment.json" \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc3} {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff  {subdir2}/{sc3}.nxs '
                        '-o {subdir2}/{sc3}.attachment.json \n'
                        'INFO : DatasetIngestor: Metadata generated callback: '
                        'echo " APPEND {subdir2}/{sc3}.scan.json '
                        '{subdir2}/{sc3}.origdatablock.json '
                        '{subdir2}/{sc3}.attachment.json" \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc4} {subdir2}/{sc4}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff  {subdir2}/{sc4}.nxs '
                        '-o {subdir2}/{sc4}.attachment.json \n'
                        'INFO : DatasetIngestor: Metadata generated callback: '
                        'echo " APPEND {subdir2}/{sc4}.scan.json '
                        '{subdir2}/{sc4}.origdatablock.json '
                        '{subdir2}/{sc4}.attachment.json" \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets Attachments: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n"
                    "Datasets Attachments: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {
                              'signal': 'lk_spectrum',
                              'axes': 'mot03',
                              'NX_class': 'NXdata',
                              'lkintimage': {
                                  'shape': [1, 30]
                              },
                              'lk_spectrum': {
                                  'value': spectrum,
                                  'shape': [10]
                              },
                              'mot03': {
                                  'value': mot03,
                                  'shape': [10]
                              }
                          },
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'default': 'data',
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [1, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]),
                        {
                            'thumbnail': "data:sdfsAAA%s" % i,
                            'ownerGroup': '99001234-dmgt',
                            'caption': '',
                            'datasetId':
                            '99001234/myscan_%05i' % (i + 1),
                            'accessGroups': [
                                '99001234-dmgt', '99001234-clbt',
                                '99001234-part', 'p00dmgt', 'p00staff']
                        },
                        skip=["thumbnail"])
                    self.assertTrue(
                        json.loads(
                            self.__server.attachments[i][1])["thumbnail"].
                        startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5_attachment_counter_conf(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'max_oned_size: 3\n' \
            'max_oned_dataset_generator_switch: ' \
            '" --max-oned-size {{maxonedsize}} "\n' \
            'oned_in_metadata: true\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'add_empty_units: false\n' \
            'log_generator_commands: true\n' \
            'attachment_signal_names: "nonexist,lk_spectrum"\n' \
            'attachment_axes_names: "nonexist,mot03"\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r40 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r40 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]
                    mot03 = list(range(3, 13))

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    # rt.attributes.create(
                    #      "default", "string").write("entry12345")
                    entry = rt.create_group("entry12345", "NXentry")
                    # entry.attributes.create(
                    #     "default", "string").write("data")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    # dt.attributes.create(
                    #        "signal", "string").write("lk_spectrum")
                    # dt.attributes.create("axes", "string").write("mot03")
                    mt = dt.create_field("mot03", "uint32", [10], [10])
                    mt.write(mot03)

                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field(
                        "intimage", "uint32", [1, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    filewriter.link(
                        "/entry12345/instrument/detector/spectrum", dt,
                        "lk_spectrum")

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.nxs -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc3} {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc3}.attachment.json  '
                        '{subdir2}/{sc3}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{proposalId}}.{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.nxs -r raw/special  --oned  '
                        '--max-oned-size 3  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc4} {subdir2}/{sc4}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc4}.attachment.json  '
                        '{subdir2}/{sc4}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03 \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets Attachments: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n"
                    "Datasets Attachments: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99991173.99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {
                              'NX_class': 'NXdata',
                              'lkintimage': {
                                  'shape': [1, 30]
                              },
                              'lk_spectrum': {
                                  'value': [min(spectrum), max(spectrum)],
                                  'shape': [10]
                              },
                              'mot03': {
                                  'value': [min(mot03), max(mot03)],
                                  'shape': [10]
                              }
                          },
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [1, 30]
                                  },
                                  'spectrum': {
                                      'value': [min(spectrum), max(spectrum)],
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]),
                        {
                            'thumbnail': "data:sdfsAAA%s" % i,
                            'ownerGroup': '99001234-dmgt',
                            'caption': '',
                            'datasetId':
                            '99001234/myscan_%05i' % (i + 1),
                            'accessGroups': [
                                '99001234-dmgt', '99001234-clbt',
                                '99001234-part', 'p00dmgt', 'p00staff']
                        },
                        skip=["thumbnail"])
                    self.assertTrue(
                        json.loads(
                            self.__server.attachments[i][1])["thumbnail"].
                        startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5_attachment_counter_override(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'override_attachment_signals: true\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'override_attachment_signals_generator_switch: " --override "\n' \
            'log_generator_commands: true\n' \
            'add_empty_units: False\n' \
            'attachment_signal_names: "nonexist,lk_spectrum"\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'attachment_axes_names: "nonexist,mot03"\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r40 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r40 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]
                    mot03 = list(range(3, 13))

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    rt.attributes.create(
                        "default", "string").write("entry12345")
                    entry = rt.create_group("entry12345", "NXentry")
                    entry.attributes.create(
                        "default", "string").write("data")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    dt.attributes.create(
                        "signal", "string").write("lkintimage")
                    dt.attributes.create("axes", "string").write("lk_spectrum")
                    mt = dt.create_field("mot03", "uint32", [10], [10])
                    mt.write(mot03)

                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field(
                        "intimage", "uint32", [1, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    filewriter.link(
                        "/entry12345/instrument/detector/spectrum", dt,
                        "lk_spectrum")

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00001  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc1}.attachment.json  '
                        '{subdir2}/{sc1}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03  '
                        '--override \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00002  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc2}.attachment.json  '
                        '{subdir2}/{sc2}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03  '
                        '--override \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00003  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc3} {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc3}.attachment.json  '
                        '{subdir2}/{sc3}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03  '
                        '--override \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {btmeta} '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.nxs -r raw/special  --oned  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001234/myscan_00004  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc4} {subdir2}/{sc4}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'nxsfileinfo attachment  -w 99001234-dmgt '
                        '-c 99001234-dmgt,99001234-clbt,99001234-part,'
                        'p00dmgt,p00staff '
                        ' -n \'\' -o {subdir2}/{sc4}.attachment.json  '
                        '{subdir2}/{sc4}.nxs '
                        '-s nonexist,lk_spectrum  -e nonexist,mot03  '
                        '--override \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets Attachments: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n"
                    "Datasets Attachments: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {
                              'signal': 'lkintimage',
                              'axes': 'lk_spectrum',
                              'NX_class': 'NXdata',
                              'lkintimage': {
                                  'shape': [1, 30]
                              },
                              'lk_spectrum': {
                                  'value': spectrum,
                                  'shape': [10]
                              },
                              'mot03': {
                                  'value': mot03,
                                  'shape': [10]
                              }
                          },
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'default': 'data',
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [1, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]),
                        {
                            'thumbnail': "data:sdfsAAA%s" % i,
                            'ownerGroup': '99001234-dmgt',
                            'caption': '',
                            'datasetId':
                            '99001234/myscan_%05i' % (i + 1),
                            'accessGroups': [
                                '99001234-dmgt', '99001234-clbt',
                                '99001234-part', 'p00dmgt', 'p00staff']
                        },
                        skip=["thumbnail"])
                    self.assertTrue(
                        json.loads(
                            self.__server.attachments[i][1])["thumbnail"].
                        startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5_attachment_image(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'oned_in_metadata: true\n' \
            'oned_dataset_generator_switch: " --oned "\n' \
            'add_empty_units: False\n' \
            'attachment_signal_names: "nonexist,lambda"\n' \
            'attachment_image_frame_number: 2\n' \
            'ingest_dataset_attachment: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r40 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r40 -l debug'
                     % cfgfname).split()]
        c2d = [
            np.random.normal(0, 0.1, 1024).reshape(16, 64),
            np.random.normal(0.01, 0.11, 1024).reshape(16, 64),
            np.random.normal(0.02, 0.12, 1024).reshape(16, 64),
            np.random.normal(0.03, 0.13, 1024).reshape(16, 64),
            np.random.normal(0.04, 0.14, 1024).reshape(16, 64),
            np.random.normal(0.05, 0.15, 1024).reshape(16, 64)
        ]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]
                    mot03 = list(range(3, 13))

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    # rt.attributes.create(
                    #         "default", "string").write("entry12345")
                    entry = rt.create_group("entry12345", "NXentry")
                    # entry.attributes.create(
                    #      "default", "string").write("data")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    dt = entry.create_group("data", "NXdata")
                    c2 = dt.create_field(
                        "lambda", "float64", [6, 16, 64], [1, 16, 64])
                    for di, d2 in enumerate(c2d):
                        c2[di, :, :] = d2
                    # dt.attributes.create("signal", "string").\
                    #     write("lk_spectrum")
                    # dt.attributes.create("axes", "string").write("mot03")
                    mt = dt.create_field("mot03", "uint32", [10], [10])
                    mt.write(mot03)

                    sample = entry.create_group("sample", "NXsample")
                    fl = det.create_field(
                        "intimage", "uint32", [1, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)
                    fl[0, :] = list(range(30))
                    filewriter.link(
                        "/entry12345/instrument/detector/intimage", dt,
                        "lkintimage")
                    filewriter.link(
                        "/entry12345/instrument/detector/spectrum", dt,
                        "lk_spectrum")

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc3} {subdir2}/{sc3}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc4} {subdir2}/{sc4}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 99001234/myscan_00001\n"
                    "Datasets Attachments: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 99001234/myscan_00002\n"
                    "Datasets Attachments: 99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 99001234/myscan_00003\n"
                    "Datasets Attachments: 99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 99001234/myscan_00004\n"
                    "Datasets Attachments: 99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99991173.99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {
                              'NX_class': 'NXdata',
                              'lkintimage': {
                                  'shape': [1, 30]
                              },
                              'lk_spectrum': {
                                  'value': spectrum,
                                  'shape': [10]
                              },
                              'lambda': {
                                  'shape': [6, 16, 64],
                              },
                              'mot03': {
                                  'value': mot03,
                                  'shape': [10]
                              }
                          },
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [1, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt', '99001234-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                self.assertEqual(len(self.__server.attachments), 4)
                for i in range(4):
                    self.assertEqual(len(self.__server.attachments[i]), 2)
                    self.assertEqual(self.__server.attachments[i][0],
                                     '99001234/myscan_%05i' % (i + 1))
                    self.myAssertDict(
                        json.loads(self.__server.attachments[i][1]),
                        {
                            'thumbnail': "data:sdfsAAA%s" % i,
                            'ownerGroup': '99001234-dmgt',
                            'caption': '',
                            'datasetId':
                            '99001234/myscan_%05i' % (i + 1),
                            'accessGroups': [
                                '99001234-dmgt', '99001234-clbt',
                                '99001234-part', 'p00dmgt', 'p00staff']
                        },
                        skip=["thumbnail"])
                    self.assertTrue(
                        json.loads(
                            self.__server.attachments[i][1])["thumbnail"].
                        startswith("data:image/png;base64,i"))
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_corepath(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirnames = os.path.abspath(os.path.join(dirname, "scratch"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        coredir = "/tmp/scingestor_core_%s" % uuid.uuid4().hex
        cfsubdirname = os.path.abspath(os.path.join(coredir, "raw"))
        cfsubdirnames = os.path.abspath(os.path.join(coredir, "scratch"))
        cfsubdirname2 = os.path.abspath(os.path.join(cfsubdirname, "special"))
        cfsubdirname3 = os.path.abspath(os.path.join(cfsubdirname2, "scansub"))
        btmeta = "beamtime-metadata-99001284.json"
        dslist = "scicat-datasets-99001284.lst"
        idslist = "scicat-ingested-datasets-99001284.lst"
        wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        with open(source) as blf:
            jblm = blf.read()
            blm = json.loads(jblm)
            blm["corePath"] = coredir
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        wlsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                "config",
                                wrongdslist)
        fullbtmeta = os.path.join(fdirname, btmeta)
        # fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        cfullbtmeta = os.path.join(coredir, btmeta)
        cfdslist = os.path.join(cfsubdirname2, dslist)
        cfidslist = os.path.join(cfsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        os.mkdir(fdirname)
        os.mkdir(fsubdirnames)
        os.makedirs(coredir, exist_ok=True)
        os.mkdir(cfsubdirnames)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule
        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)

        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
                cfsubdirnames,
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n',
                fsubdirnames,
            ],
        ]
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)
                os.mkdir(cfsubdirname)
                os.mkdir(cfsubdirname2)
                os.mkdir(cfsubdirname3)
                for k, arg in enumerate(args):
                    cfg = 'beamtime_dirs:\n' \
                        '  - "{basedir}"\n' \
                        'scandir_blacklist:\n' \
                        '  - "{scratchdir}"\n' \
                        'scicat_url: "{url}"\n' \
                        'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
                        'chmod_json_files: "{chmod}"\n' \
                        'log_generator_commands: true\n' \
                        'use_corepath_as_scandir: true\n' \
                        'add_empty_units: true\n' \
                        'add_empty_units_generator_switch: ' \
                        '" --add-empty-units "\n' \
                        'ingest_dataset_attachment: false\n' \
                        'chmod_generator_switch: " -x {{chmod}} "\n' \
                        'ingestor_var_dir: "{vardir}"\n' \
                        'ingestor_credential_file: "{credfile}"\n'.format(
                            basedir=fdirname, url=url, vardir=vardir,
                            # scratchdir=cfsubdirnames,
                            scratchdir=arg[10],
                            credfile=credfile, chmod=chmod)

                    with open(cfgfname, "w+") as cf:
                        cf.write(cfg)

                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    cnxsfilename = os.path.join(cfsubdirname2, arg[0])
                    # dsfilename = nxsfilename[:-4] + ".scan.json"
                    cdbfilename = cnxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                    shutil.copy(nxsfilename, cfsubdirname2)
                #  shutil.copy(source, fdirname)
                with open(cfullbtmeta, "w") as blf:
                    blf.write(json.dumps(blm))
                with open(fullbtmeta, "w") as blf:
                    blf.write(json.dumps(blm))
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                shutil.copy(lsource, cfsubdirname2)
                shutil.copy(wlsource, cfsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    cnxsfilename = os.path.join(cfsubdirname2, arg[0])
                    cdsfilename = cnxsfilename[:-4] + ".scan.json"
                    cdbfilename = cnxsfilename[:-4] + ".origdatablock.json"
                    status = os.stat(cdsfilename)
                    self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                    status = os.stat(cdbfilename)
                    self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{cbasedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {cbtmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {cbtmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00001  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  -x 0o662  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00001  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00002  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  -x 0o662  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00002  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '-x 0o662  {subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{cbasedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{cbtmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{cbtmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, cbasedir=coredir,
                                btmeta=fullbtmeta,
                                cbtmeta=cfullbtmeta,
                                subdir=cfsubdirname, subdir2=cfsubdirname2,
                                dslist=cfdslist, idslist=cfidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001284/myscan_00001\n"
                    "OrigDatablocks: 99001284/myscan_00001\n"
                    "Datasets: 99001284/myscan_00002\n"
                    "OrigDatablocks: 99001284/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001284-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001284/myscan_00001',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001284',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                          'value': args[0][9],
                          'unit': '',
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {
                          'value': '%s' % args[0][6],
                          'unit': ''},
                      'experiment_identifier': {
                          'value': '%s' % args[0][2],
                          'unit': ''},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[0][4],
                              'value': '%s' % args[0][3],
                              'unit': ''
                          }},
                      'sample': {
                        'NX_class': 'NXsample',
                        'chemical_formula': {
                            'value': '%s' % args[0][8],
                            'unit': ''},
                        'name': {
                            'value': '%s' % args[0][7],
                            'unit': ''}},
                      'start_time': {
                          'value': '%s' % args[0][5],
                          'unit': ''},
                      'title': {
                          'value': '%s' % args[0][1],
                          'unit': ''},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001284'},
                     'sourceFolder':
                     '%s/raw/special' % coredir,
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001284-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001284/myscan_00002',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001284',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                          'value':  args[1][9],
                          'unit': ''
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[1][6],
                                   'unit': ''},
                      'experiment_identifier': {'value': '%s' % args[1][2],
                                                'unit': ''},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3],
                              'unit': ''}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[1][8],
                                               'unit': ''},
                          'name': {'value': '%s' % args[1][7],
                                   'unit': ''}},
                      'start_time': {
                          'value': '%s' % args[1][5],
                          'unit': ''},
                      'title': {'value': '%s' % args[1][1],
                                'unit': ''},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001284'},
                     'sourceFolder':
                     '%s/raw/special' % coredir,
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001284-dmgt',
                     'datasetId': '99001284/myscan_00001',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001284-dmgt',
                     'datasetId': '99001284/myscan_00002',
                     'accessGroups': [
                         '99001284-dmgt', '99001284-clbt', '99001284-part',
                         'p00dmgt', 'p00staff'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(cfsubdirname):
                    shutil.rmtree(cfsubdirname)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(coredir):
                shutil.rmtree(coredir)

    def test_datasetfile_add_h5_corepath(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        coredir = "/tmp/scingestor_core_%s" % uuid.uuid4().hex
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirnames = os.path.abspath(os.path.join(dirname, "scratch"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        cfsubdirname = os.path.abspath(os.path.join(coredir, "raw"))
        cfsubdirnames = os.path.abspath(os.path.join(coredir, "scratch"))
        cfsubdirname2 = os.path.abspath(os.path.join(cfsubdirname, "special"))
        cfsubdirname3 = os.path.abspath(os.path.join(cfsubdirname2, "scansub"))
        os.mkdir(fdirname)
        os.makedirs(coredir, exist_ok=True)
        btmeta = "beamtime-metadata-99001284.json"
        dslist = "scicat-datasets-99001284.lst"
        idslist = "scicat-ingested-datasets-99001284.lst"
        # wrongdslist = "scicat-datasets-99001235.lst"
        source = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "config",
                              btmeta)
        cfullbtmeta = os.path.join(coredir, btmeta)
        fullbtmeta = os.path.join(dirname, btmeta)
        with open(source) as blf:
            jblm = blf.read()
            blm = json.loads(jblm)
            blm["corePath"] = coredir
        lsource = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               "config",
                               dslist)
        # shutil.copy(source, fdirname)
        with open(cfullbtmeta, "w") as blf:
            blf.write(json.dumps(blm))
        with open(fullbtmeta, "w") as blf:
            blf.write(json.dumps(blm))
        # shutil.copy(lsource, fsubdirname2)
        # shutil.copy(wlsource, fsubdirname)
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        cfullbtmeta = os.path.join(coredir, btmeta)
        cfdslist = os.path.join(cfsubdirname2, dslist)
        cfidslist = os.path.join(cfsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            shutil.copy(lsource, cfsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            os.mkdir(cfsubdirname3)
            os.mkdir(fsubdirnames)
            os.mkdir(cfsubdirnames)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")
            shutil.copy(fdslist, cfsubdirname2)

        # commands.pop()
        try:
            for kk, cmd in enumerate(commands):
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(cfsubdirname)
                os.mkdir(cfsubdirname2)

                if kk % 2:
                    scratchdir = cfsubdirnames
                else:
                    scratchdir = fsubdirnames

                cfg = 'beamtime_dirs:\n' \
                    '  - "{basedir}"\n' \
                    'scandir_blacklist:\n' \
                    '  - "{scratchdir}"\n' \
                    'scicat_url: "{url}"\n' \
                    'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
                    'oned_in_metadata: true\n' \
                    'ingest_dataset_attachment: false\n' \
                    'log_generator_commands: true\n' \
                    'use_corepath_as_scandir: true\n' \
                    'oned_dataset_generator_switch: " --oned "\n' \
                    'ingestor_var_dir: "{vardir}"\n' \
                    'ingestor_username: "{username}"\n' \
                    'ingestor_credential_file: "{credfile}"\n'.format(
                        scratchdir=scratchdir,
                        basedir=fdirname, url=url, vardir=vardir,
                        username=username, credfile=credfile)

                with open(cfgfname, "w+") as cf:
                    cf.write(cfg)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()
                    shutil.copy(nxsfilename, cfsubdirname2)

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(lsource, cfsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{cbasedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {cbtmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {cbtmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00001  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc1}.nxs -r raw/special  --oned  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00001  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00002  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc2}.nxs -r raw/special  --oned  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00002  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00003  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc3}.nxs -r raw/special  --oned  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00003  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata -k4  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '--id-format \'{{beamtimeId}}\' '
                        '-z \'\' -e \'\' -b {cbtmeta} '
                        '-p 99001284/myscan_00004  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '{subdir2}/{sc4}.nxs -r raw/special  --oned  '
                        '--add-empty-units  \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,'
                        '*.attachment.json,*~  '
                        ' -r \'\'  '
                        '-p 99001284/myscan_00004  -w 99001284-dmgt '
                        '-c 99001284-dmgt,99001284-clbt,99001284-part,'
                        'p00dmgt,p00staff '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '99001284/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '99001284/{sc4}\n'
                        # 'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        # '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{cbasedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{cbtmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{cbtmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(
                            basedir=fdirname, cbasedir=coredir,
                            btmeta=fullbtmeta,
                            cbtmeta=cfullbtmeta,
                            subdir=cfsubdirname, subdir2=cfsubdirname2,
                            dslist=cfdslist, idslist=cfidslist,
                            cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                            cnt4=(cnt + 3), cnt5=(cnt + 4),
                            sc1='myscan_00001', sc2='myscan_00002',
                            sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001284/myscan_00001\n"
                    "OrigDatablocks: 99001284/myscan_00001\n"
                    "Datasets: 99001284/myscan_00002\n"
                    "OrigDatablocks: 99001284/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001284/myscan_00003\n"
                    "OrigDatablocks: 99001284/myscan_00003\n"
                    "Datasets: 99001284/myscan_00004\n"
                    "OrigDatablocks: 99001284/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001284/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001284-dmgt', '99001284-clbt', '99001284-part',
                             'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001284-dmgt',
                         'proposalId': '99001284',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9],
                              'unit': ''
                          },
                          'data': {'NX_class': 'NXdata'},
                          'end_time': {
                              'value': '%s' % arg[6],
                              'unit': ''},
                          'experiment_identifier': {
                              'value': '%s' % arg[2],
                              'unit': ''},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [0, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10],
                                      'unit': ''
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3],
                                  'unit': ''}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {
                                  'value': '%s' % arg[8],
                                  'unit': ''},
                              'name': {
                                  'value': '%s' % arg[7],
                                  'unit': ''}},
                          'start_time': {
                              'value': '%s' % arg[5],
                              'unit': ''},
                          'title': {
                              'value': '%s' % arg[1],
                              'unit': ''},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001284'},
                         'sourceFolder':
                         '%s/raw/special' % coredir,
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001284-dmgt',
                         'datasetId':
                         '99001284/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001284-dmgt', '99001284-clbt', '99001284-part',
                             'p00dmgt', 'p00staff'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
                if os.path.isdir(cfsubdirname):
                    shutil.rmtree(cfsubdirname)
                if os.path.isdir(fsubdirnames):
                    shutil.rmtree(fsubdirnames)
                if os.path.isdir(cfsubdirnames):
                    shutil.rmtree(cfsubdirnames)
        finally:
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)
            if os.path.isdir(coredir):
                shutil.rmtree(coredir)

    def test_datasetfile_exist_h5_script(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        chmod = "0o662"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        oldpidprefix = self.__server.pidprefix
        self.__server.pidprefix = "10.3204/"

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'dataset_pid_prefix: "10.3204/"\n' \
            'log_generator_commands: true\n' \
            'ingest_dataset_attachment: false\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'file_dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scanpostfix}} ' \
            ' -k3 -x 0o662 ' \
            ' -r {{relpath}} ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} ' \
            '{{scanpath}}/{{scanname}}.nxs"\n' \
            'datablock_metadata_generator: "nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -x 0o662 ' \
            ' -p {{pidprefix}}{{beamtimeid}}/{{scanname}} ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt ' \
            ' -o {{scanpath}}/{{scanname}}{{datablockpostfix}} "\n' \
            'datablock_metadata_stream_generator: ' \
            'nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -x 0o662 ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt' \
            ' -p {{pidprefix}}{{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            '" {{scanpath}}/{{scanname}} "\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]
        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    dsfilename = nxsfilename[:-4] + ".scan.json"
                    dbfilename = nxsfilename[:-4] + ".origdatablock.json"
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                status = os.stat(dsfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                status = os.stat(dbfilename)
                self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '-k3 -x 0o662  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00001 '
                        '{subdir2}/{sc1}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-x 0o662  '
                        '-p 10.3204/99001234/myscan_00001  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '-k3 -x 0o662  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00002 '
                        '{subdir2}/{sc2}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-x 0o662  '
                        '-p 10.3204/99001234/myscan_00002  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2} \n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt',
                         '99001234-part', 'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt',
                         '99001234-part', 'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     'type': 'raw',
                     'updatedAt': '2022-05-14 11:54:29'})
                self.assertEqual(len(self.__server.origdatablocks), 2)
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[0]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '10.3204/99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'size': 629}, skip=["dataFileList", "size"])
                self.myAssertDict(
                    json.loads(self.__server.origdatablocks[1]),
                    {'dataFileList': [
                        {'gid': 'jkotan',
                         'path': 'myscan_00001.scan.json',
                         'perm': '-rw-r--r--',
                         'size': 629,
                         'time': '2022-07-05T19:07:16.683673+0200',
                         'uid': 'jkotan'}],
                     'ownerGroup': '99001234-dmgt',
                     'datasetId': '10.3204/99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                     'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_bad_script(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        oldpidprefix = self.__server.pidprefix
        self.__server.pidprefix = "10.3204/"

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_pid_prefix: "10.3204/"\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingest_dataset_attachment: true\n' \
            'attachment_metadata_generator: "true"\n'\
            'file_dataset_metadata_generator: "true"\n' \
            'datablock_metadata_generator: "true"\n' \
            'datablock_metadata_stream_generator: "true"\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            '"  "\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                # status = os.stat(dsfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                # status = os.stat(dbfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'true \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'true  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'true\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'true \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'true  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'true\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
                self.assertEqual(len(self.__server.attachments), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_bad_meta_script(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        oldpidprefix = self.__server.pidprefix
        self.__server.pidprefix = "10.3204/"

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_pid_prefix: "10.3204/"\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingest_dataset_attachment: true\n' \
            'attachment_metadata_generator: ' \
            'echo "sdfs:[" > {{scanpath}}/{{scanname}}.attachment.json \n'\
            'file_dataset_metadata_generator: ' \
            'echo "blaha:[" > {{scanpath}}/{{scanname}}.scan.json \n'\
            'datablock_metadata_generator_scanpath_postfix: '\
            '"  "\n' \
            'datablock_metadata_generator: ' \
            'echo "haha:[" > {{scanpath}}/{{scanname}}.origdatablock.json \n'\
            'datablock_metadata_stream_generator: ' \
            'echo "hihi:[" \n'\
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]
        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                # status = os.stat(dsfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                # status = os.stat(dbfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'echo "blaha:[" > {subdir2}/{sc1}.scan.json \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'echo "haha:[" > '
                        '{subdir2}/{sc1}.origdatablock.json  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'echo "sdfs:[" > {subdir2}/{sc1}.attachment.json\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'echo "blaha:[" > {subdir2}/{sc2}.scan.json \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'echo "haha:[" > '
                        '{subdir2}/{sc2}.origdatablock.json  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'echo "sdfs:[" > {subdir2}/{sc2}.attachment.json\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 0)
                self.assertEqual(len(self.__server.origdatablocks), 0)
                self.assertEqual(len(self.__server.attachments), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_exist_h5_bad_meta_script2(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
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
        fullbtmeta = os.path.join(fdirname, btmeta)
        fdslist = os.path.join(fsubdirname2, dslist)
        fidslist = os.path.join(fsubdirname2, idslist)
        credfile = os.path.join(fdirname, 'pwd')
        url = 'http://localhost:8881'
        vardir = "/"
        cred = "12342345"
        os.mkdir(fdirname)
        with open(credfile, "w") as cf:
            cf.write(cred)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        oldpidprefix = self.__server.pidprefix
        self.__server.pidprefix = "10.3204/"

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'dataset_pid_prefix: "10.3204/"\n' \
            'log_generator_commands: true\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingest_dataset_attachment: true\n' \
            'file_dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scanpostfix}} ' \
            ' -x 0o662 ' \
            ' -r {{relpath}} ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} ' \
            '{{scanpath}}/{{scanname}}.nxs"\n' \
            'attachment_metadata_generator: ' \
            'echo "sdfs:[" > {{scanpath}}/{{scanname}}.attachment.json \n'\
            'datablock_metadata_generator_scanpath_postfix: '\
            '"  "\n' \
            'datablock_metadata_generator: ' \
            'echo "haha:[" > {{scanpath}}/{{scanname}}.origdatablock.json \n'\
            'datablock_metadata_stream_generator: ' \
            'echo "hihi:[" \n'\
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)
        commands = [('scicat_dataset_ingestor -c %s -r10 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r10 -l debug'
                     % cfgfname).split()]
        # commands.pop()

        args = [
            [
                "myscan_00001.nxs",
                "Test experiment",
                "BL1234554",
                "PETRA III",
                "P3",
                "2014-02-12T15:19:21+00:00",
                "2014-02-15T15:17:21+00:00",
                "water",
                "H20",
                'technique: "saxs"',
            ],
            [
                "myscan_00002.nxs",
                "My experiment",
                "BT123_ADSAD",
                "Petra III",
                "PIII",
                "2019-02-14T15:19:21+00:00",
                "2019-02-15T15:27:21+00:00",
                "test sample",
                "LaB6",
                'techniques_pids:\n'
                '  - "PaNET01191"\n'
                '  - "PaNET01188"\n'
                '  - "PaNET01098"\n'
            ],
        ]

        ltechs = [
            [
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                }
            ],
            [
                {
                    'name': 'wide angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01191'
                },
                {
                    'name': 'small angle x-ray scattering',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01188'
                },
                {
                    'name': 'grazing incidence diffraction',
                    'pid':
                    'http://purl.org/pan-science/PaNET/PaNET01098'
                },
            ],

        ]

        try:
            for cmd in commands:
                time.sleep(1)
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)
                os.mkdir(fsubdirname3)

                for k, arg in enumerate(args):
                    nxsfilename = os.path.join(fsubdirname2, arg[0])
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                shutil.copy(source, fdirname)
                shutil.copy(lsource, fsubdirname2)
                shutil.copy(wlsource, fsubdirname)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                vl, er = self.runtest(cmd)
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]

                # status = os.stat(dsfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))
                # status = os.stat(dbfilename)
                # self.assertEqual(chmod, str(oct(status.st_mode & 0o777)))

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
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '-x 0o662  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00001 '
                        '{subdir2}/{sc1}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'echo "haha:[" > '
                        '{subdir2}/{sc1}.origdatablock.json  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc1} {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'echo "sdfs:[" > {subdir2}/{sc1}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '-x 0o662  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00002 '
                        '{subdir2}/{sc2}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'echo "haha:[" > '
                        '{subdir2}/{sc2}.origdatablock.json  \n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment metadata:'
                        ' {sc2} {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating attachment command: '
                        'echo "sdfs:[" > {subdir2}/{sc2}.attachment.json\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'ERROR : DatasetIngestor: Expecting value: '
                        'line 1 column 1 (char 0)\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002'),
                        '\n'.join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    "Login: ingestor\n"
                    "Datasets: 99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n", vl)
                self.assertEqual(len(self.__server.userslogin), 1)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "ingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 2)
                self.myAssertDict(
                    json.loads(self.__server.datasets[0]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[0][6],
                     # 'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[0][1],
                     'endTime': args[0][6],
                     'isPublished': False,
                     'techniques': ltechs[0],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00001',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt',
                         '99001234-part', 'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00001',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                        'value': args[0][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[0][6]},
                      'experiment_identifier': {'value': '%s' % args[0][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                            'short_name': '%s' % args[0][4],
                            'value': '%s' % args[0][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[0][8]},
                          'name': {'value': '%s' % args[0][7]}},
                      'start_time': {
                          'value': '%s' % args[0][5]},
                      'title': {'value': '%s' % args[0][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     # 'updatedAt': '2022-05-14 11:54:29',
                     'type': 'raw'})
                self.myAssertDict(
                    json.loads(self.__server.datasets[1]),
                    {'contactEmail': 'appuser@fake.com',
                     'creationTime': args[1][6],
                     # 'createdAt': '2022-05-14 11:54:29',
                     'instrumentId': '/petra3/p00',
                     'creationLocation': '/DESY/PETRA III/P00',
                     'description': args[1][1],
                     'endTime': args[1][6],
                     'isPublished': False,
                     'techniques': ltechs[1],
                     'owner': 'Smithson',
                     'keywords': ['scan'],
                     'ownerGroup': '99001234-dmgt',
                     'ownerEmail': 'peter.smithson@fake.de',
                     'pid': '99001234/myscan_00002',
                     'accessGroups': [
                         '99001234-dmgt', '99001234-clbt',
                         '99001234-part', 'p00dmgt', 'p00staff'],
                     'datasetName': 'myscan_00002',
                     'principalInvestigator': 'appuser@fake.com',
                     'proposalId': '99001234',
                     'scientificMetadata':
                     {'name': 'entry12345',
                      'experiment_description': {
                        'value':  args[1][9]
                      },
                      'data': {'NX_class': 'NXdata'},
                      'end_time': {'value': '%s' % args[1][6]},
                      'experiment_identifier': {'value': '%s' % args[1][2]},
                      'instrument': {
                          'NX_class': 'NXinstrument',
                          'detector': {
                              'NX_class': 'NXdetector',
                              'intimage': {
                                  'shape': [0, 30]}},
                          'name': {
                              'short_name': '%s' % args[1][4],
                              'value': '%s' % args[1][3]}},
                      'sample': {
                        'NX_class': 'NXsample',
                          'chemical_formula': {'value': '%s' % args[1][8]},
                          'name': {'value': '%s' % args[1][7]}},
                      'start_time': {
                          'value': '%s' % args[1][5]},
                      'title': {'value': '%s' % args[1][1]},
                      'DOOR_proposalId': '99991173',
                      'beamtimeId': '99001234'},
                     'sourceFolder':
                     '/asap3/petra3/gpfs/p00/2022/data/9901234/raw/special',
                     # 'updatedAt': '2022-05-14 11:54:29',
                     'type': 'raw'})
                self.assertEqual(len(self.__server.origdatablocks), 0)
                self.assertEqual(len(self.__server.attachments), 0)
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)

    def test_datasetfile_add_h5_script(self):
        fun = sys._getframe().f_code.co_name
        # print("Run: %s.%s() " % (self.__class__.__name__, fun))
        dirname = "test_current"
        while os.path.exists(dirname):
            dirname = dirname + '_1'
        fdirname = os.path.abspath(dirname)
        fsubdirname = os.path.abspath(os.path.join(dirname, "raw"))
        fsubdirname2 = os.path.abspath(os.path.join(fsubdirname, "special"))
        fsubdirname3 = os.path.abspath(os.path.join(fsubdirname2, "scansub"))
        os.mkdir(fdirname)
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
        vardir = "/"
        cred = "12342345"
        username = "myingestor"
        with open(credfile, "w") as cf:
            cf.write(cred)

        oldpidprefix = self.__server.pidprefix
        self.__server.pidprefix = "10.3204/"

        cfg = 'beamtime_dirs:\n' \
            '  - "{basedir}"\n' \
            'scicat_url: "{url}"\n' \
            'dataset_pid_prefix: "10.3204/"\n' \
            'log_generator_commands: true\n' \
            'ingest_dataset_attachment: false\n' \
            'scicat_proposal_id_pattern: "{{beamtimeid}}"\n' \
            'oned_in_metadata: true\n' \
            'file_dataset_metadata_generator: "nxsfileinfo metadata ' \
            ' -o {{scanpath}}/{{scanname}}{{scanpostfix}} ' \
            ' -x 0o662 ' \
            ' --oned ' \
            ' -r {{relpath}} ' \
            ' -b {{beamtimefile}} -p {{beamtimeid}}/{{scanname}} ' \
            '{{scanpath}}/{{scanname}}.nxs"\n' \
            'datablock_metadata_generator: "nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -p {{pidprefix}}{{beamtimeid}}/{{scanname}} ' \
            ' -x 0o662 ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt ' \
            ' -o {{scanpath}}/{{scanname}}{{datablockpostfix}} "\n' \
            'datablock_metadata_stream_generator: ' \
            'nxsfileinfo origdatablock ' \
            ' -s *.pyc,*{{datablockpostfix}},*{{scanpostfix}},*~ ' \
            ' -c {{beamtimeid}}-clbt,{{beamtimeid}}-dmgt,{{beamline}}dmgt' \
            ' -x 0o662 ' \
            ' -p {{pidprefix}}{{beamtimeid}}/{{scanname}} "\n' \
            'datablock_metadata_generator_scanpath_postfix: '\
            ' " {{scanpath}}/{{scanname}}"\n' \
            'ingestor_var_dir: "{vardir}"\n' \
            'ingestor_username: "{username}"\n' \
            'ingestor_credential_file: "{credfile}"\n'.format(
                basedir=fdirname, url=url, vardir=vardir,
                username=username, credfile=credfile)

        cfgfname = "%s_%s.yaml" % (self.__class__.__name__, fun)
        with open(cfgfname, "w+") as cf:
            cf.write(cfg)

        wrmodule = WRITERS[self.writer]
        filewriter.writer = wrmodule

        commands = [('scicat_dataset_ingestor -c %s -r36 --log debug'
                     % cfgfname).split(),
                    ('scicat_dataset_ingestor --config %s -r36 -l debug'
                     % cfgfname).split()]

        arg = [
            "myscan_%05i.nxs",
            "Test experiment",
            "BL1234554",
            "PETRA III",
            "P3",
            "2014-02-12T15:19:21+00:00",
            "2014-02-15T15:17:21+00:00",
            "water",
            "H20",
            'technique: "saxs"',
            'sampleId: "water/hh/1231321"',
        ]
        ltech = [
            {
                'name': 'small angle x-ray scattering',
                'pid':
                'http://purl.org/pan-science/PaNET/PaNET01188'
            }
        ]
        sid = "water/hh/1231321"

        def tst_thread():
            """ test thread which adds and removes beamtime metadata file """
            time.sleep(3)
            shutil.copy(lsource, fsubdirname2)
            time.sleep(5)
            os.mkdir(fsubdirname3)
            time.sleep(12)
            with open(fdslist, "a+") as fds:
                fds.write("myscan_00003\n")
                fds.write("myscan_00004\n")

        # commands.pop()
        try:
            for cmd in commands:
                os.mkdir(fsubdirname)
                os.mkdir(fsubdirname2)

                for k in range(4):
                    nxsfilename = os.path.join(
                        fsubdirname2, arg[0] % (k + 1))
                    title = arg[1]
                    beamtime = arg[2]
                    insname = arg[3]
                    inssname = arg[4]
                    stime = arg[5]
                    etime = arg[6]
                    smpl = arg[7]
                    formula = arg[8]
                    sdesc = arg[10]
                    spectrum = [243, 34, 34, 23, 334, 34, 34, 33, 32, 11]

                    nxsfile = filewriter.create_file(
                        nxsfilename, overwrite=True)
                    rt = nxsfile.root()
                    entry = rt.create_group("entry12345", "NXentry")
                    ins = entry.create_group("instrument", "NXinstrument")
                    det = ins.create_group("detector", "NXdetector")
                    entry.create_field(
                        "experiment_description", "string").write(arg[9])
                    entry.create_group("data", "NXdata")
                    sample = entry.create_group("sample", "NXsample")
                    det.create_field("intimage", "uint32", [0, 30], [1, 30])
                    sp = det.create_field("spectrum", "uint32", [10], [10])
                    sp.write(spectrum)

                    entry.create_field("title", "string").write(title)
                    entry.create_field(
                        "experiment_identifier", "string").write(beamtime)
                    entry.create_field("start_time", "string").write(stime)
                    entry.create_field("end_time", "string").write(etime)
                    sname = ins.create_field("name", "string")
                    sname.write(insname)
                    sattr = sname.attributes.create("short_name", "string")
                    sattr.write(inssname)
                    sname = sample.create_field("name", "string")
                    sname.write(smpl)
                    sde = sample.create_field("description", "string")
                    sde.write(sdesc)
                    sfml = sample.create_field("chemical_formula", "string")
                    sfml.write(formula)
                    nxsfile.close()

                # print(cmd)
                self.notifier = safeINotifier.SafeINotifier()
                cnt = self.notifier.id_queue_counter + 1
                self.__server.reset()
                shutil.copy(lsource, fsubdirname2)
                if os.path.exists(fidslist):
                    os.remove(fidslist)
                th = threading.Thread(target=tst_thread)
                th.start()
                vl, er = self.runtest(cmd)
                th.join()
                ser = er.split("\n")
                seri = [ln for ln in ser if not ln.startswith("127.0.0.1")]
                dseri = [ln for ln in seri if "DEBUG :" not in ln]
                # sero = [ln for ln in ser if ln.startswith("127.0.0.1")]
                # print(er)
                try:
                    self.assertEqual(
                        'INFO : BeamtimeWatcher: Adding watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: Create ScanDirWatcher '
                        '{basedir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Create ScanDirWatcher '
                        '{subdir2} {btmeta}\n'
                        'INFO : ScanDirWatcher: Adding watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Creating DatasetWatcher '
                        '{dslist}\n'
                        'INFO : DatasetWatcher: Adding watch {cnt5}: '
                        '{dslist} {idslist}\n'
                        'INFO : DatasetWatcher: Waiting datasets: '
                        '[\'{sc1}\', \'{sc2}\']\n'
                        'INFO : DatasetWatcher: Ingested datasets: []\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc1}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc1} {subdir2}/{sc1}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc1}.scan.json  '
                        '-x 0o662  --oned  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00001 '
                        '{subdir2}/{sc1}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc1} {subdir2}/{sc1}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-p 10.3204/99001234/myscan_00001  '
                        '-x 0o662  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc1}.origdatablock.json  '
                        '{subdir2}/{sc1}\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc1}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc2}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc2} {subdir2}/{sc2}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc2}.scan.json  '
                        '-x 0o662  --oned  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00002 '
                        '{subdir2}/{sc2}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc2} {subdir2}/{sc2}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-p 10.3204/99001234/myscan_00002  '
                        '-x 0o662  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc2}.origdatablock.json  '
                        '{subdir2}/{sc2}\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc2}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc3}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc3} {subdir2}/{sc3}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc3}.scan.json  '
                        '-x 0o662  --oned  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00003 '
                        '{subdir2}/{sc3}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc3} {subdir2}/{sc3}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-p 10.3204/99001234/myscan_00003  '
                        '-x 0o662  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc3}.origdatablock.json  '
                        '{subdir2}/{sc3}\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc3}\n'
                        'INFO : DatasetIngestor: Ingesting: {dslist} {sc4}\n'
                        'INFO : DatasetIngestor: Generating nxs metadata: '
                        '{sc4} {subdir2}/{sc4}.scan.json\n'
                        'INFO : DatasetIngestor: Generating dataset command: '
                        'nxsfileinfo metadata  '
                        '-o {subdir2}/{sc4}.scan.json  '
                        '-x 0o662  --oned  -r raw/special  -b {btmeta} '
                        '-p 99001234/myscan_00004 '
                        '{subdir2}/{sc4}.nxs \n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock metadata:'
                        ' {sc4} {subdir2}/{sc4}.origdatablock.json\n'
                        'INFO : DatasetIngestor: '
                        'Generating origdatablock command: '
                        'nxsfileinfo origdatablock  '
                        '-s *.pyc,*.origdatablock.json,*.scan.json,*~  '
                        '-p 10.3204/99001234/myscan_00004  '
                        '-x 0o662  '
                        '-c 99001234-clbt,99001234-dmgt,p00dmgt  '
                        '-o {subdir2}/{sc4}.origdatablock.json  '
                        '{subdir2}/{sc4}\n'
                        'INFO : DatasetIngestor: Check if dataset exists: '
                        '10.3204/99001234/{sc4}\n'
                        'INFO : DatasetIngestor: Post the dataset: '
                        '10.3204/99001234/{sc4}\n'
                        'INFO : BeamtimeWatcher: Removing watch {cnt1}: '
                        '{basedir}\n'
                        'INFO : BeamtimeWatcher: '
                        'Stopping ScanDirWatcher {btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt2}: '
                        '{basedir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt3}: '
                        '{subdir}\n'
                        'INFO : ScanDirWatcher: Stopping ScanDirWatcher '
                        '{btmeta}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt4}: '
                        '{subdir2}\n'
                        'INFO : ScanDirWatcher: Stopping DatasetWatcher '
                        '{dslist}\n'
                        'INFO : ScanDirWatcher: Removing watch {cnt5}: '
                        '{dslist}\n'
                        .format(basedir=fdirname, btmeta=fullbtmeta,
                                subdir=fsubdirname, subdir2=fsubdirname2,
                                dslist=fdslist, idslist=fidslist,
                                cnt1=cnt, cnt2=(cnt + 1), cnt3=(cnt + 2),
                                cnt4=(cnt + 3), cnt5=(cnt + 4),
                                sc1='myscan_00001', sc2='myscan_00002',
                                sc3='myscan_00003', sc4='myscan_00004'),
                        "\n".join(dseri))
                except Exception:
                    print(er)
                    raise
                self.assertEqual(
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00001\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00001\n"
                    "Datasets: 99001234/myscan_00002\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00002\n"
                    'Login: myingestor\n'
                    "Datasets: 99001234/myscan_00003\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00003\n"
                    "Datasets: 99001234/myscan_00004\n"
                    "OrigDatablocks: 10.3204/99001234/myscan_00004\n", vl)
                self.assertEqual(len(self.__server.userslogin), 2)
                self.assertEqual(
                    self.__server.userslogin[0],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(
                    self.__server.userslogin[1],
                    b'{"username": "myingestor", "password": "12342345"}')
                self.assertEqual(len(self.__server.datasets), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.datasets[i]),
                        {'contactEmail': 'appuser@fake.com',
                         'creationTime': arg[6],
                         # 'createdAt': '2022-05-14 11:54:29',
                         'instrumentId': '/petra3/p00',
                         'creationLocation': '/DESY/PETRA III/P00',
                         'description': arg[1],
                         'endTime': arg[6],
                         'isPublished': False,
                         'techniques': ltech,
                         'sampleId': sid,
                         'owner': 'Smithson',
                         'keywords': ['scan'],
                         'ownerEmail': 'peter.smithson@fake.de',
                         'pid': '99001234/myscan_%05i' % (i + 1),
                         'datasetName': 'myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-dmgt', '99001234-clbt',
                             '99001234-part', 'p00dmgt', 'p00staff'],
                         'principalInvestigator': 'appuser@fake.com',
                         'ownerGroup': '99001234-dmgt',
                         'proposalId': '99001234',
                         'scientificMetadata':
                         {'name': 'entry12345',
                          'experiment_description': {
                              'value': arg[9]
                          },
                          'data': {'NX_class': 'NXdata'},
                          'end_time': {'value': '%s' % arg[6]},
                          'experiment_identifier': {'value': '%s' % arg[2]},
                          'instrument': {
                              'NX_class': 'NXinstrument',
                              'detector': {
                                  'NX_class': 'NXdetector',
                                  'intimage': {
                                      'shape': [0, 30]
                                  },
                                  'spectrum': {
                                      'value': spectrum,
                                      'shape': [10]
                                  }
                              },

                              'name': {
                                  'short_name': '%s' % arg[4],
                                  'value': '%s' % arg[3]}},
                          'sample': {
                              'NX_class': 'NXsample',
                              'chemical_formula': {'value': '%s' % arg[8]},
                              'name': {'value': '%s' % arg[7]},
                              'description': {'value': '%s' % arg[10]}},
                          'start_time': {
                              'value': '%s' % arg[5]},
                          'title': {'value': '%s' % arg[1]},
                          'DOOR_proposalId': '99991173',
                          'beamtimeId': '99001234'},
                         'sourceFolder':
                         '/asap3/petra3/gpfs/p00/2022/data/9901234/'
                         'raw/special',
                         # 'updatedAt': '2022-05-14 11:54:29',
                         'type': 'raw'})

                self.assertEqual(len(self.__server.origdatablocks), 4)
                for i in range(4):
                    self.myAssertDict(
                        json.loads(self.__server.origdatablocks[i]),
                        {'dataFileList': [
                            {'gid': 'jkotan',
                             'path': 'myscan_00001.scan.json',
                             'perm': '-rw-r--r--',
                             'size': 629,
                             'time': '2022-07-05T19:07:16.683673+0200',
                             'uid': 'jkotan'}],
                         'ownerGroup': '99001234-dmgt',
                         'datasetId':
                         '10.3204/99001234/myscan_%05i' % (i + 1),
                         'accessGroups': [
                             '99001234-clbt', '99001234-dmgt', 'p00dmgt'],
                         'size': 629}, skip=["dataFileList", "size"])
                if os.path.isdir(fsubdirname):
                    shutil.rmtree(fsubdirname)
        finally:
            self.__server.pidprefix = oldpidprefix
            if os.path.exists(cfgfname):
                os.remove(cfgfname)
            if os.path.isdir(fdirname):
                shutil.rmtree(fdirname)


if __name__ == '__main__':
    unittest.main()
