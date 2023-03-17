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
import sys
import argparse
import os
import glob
import json
import socket
import pathlib

from .configuration import load_config
from .datasetIngestor import DatasetIngestor
from .pathConverter import PathConverter
from .logger import get_logger, init_logger


class DatasetIngest:

    """ Dataset Ingest command
    """

    def __init__(self, options):
        """ constructor

        :param options: parser options
        :type options: :class:`argparse.Namespace`
        """
        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = {}
        if options.config:
            self.__config = load_config(options.config) or {}
            get_logger().debug("CONFIGURATION: %s" % str(self.__config))

        #: (:obj:`bool`) use core path
        self.__usecorepath = False
        if "use_corepath_as_scandir" in self.__config.keys():
            self.__usecorepath = bool(
                self.__config["use_corepath_as_scandir"])

        #: (:obj:`list` <:obj:`str`>) beamtime directories
        self.__beamtime_dirs = [
            # "/gpfs/current",
            # "/gpfs/commissioning",
        ]

        #: (:obj:`str`) home directory
        self.__homepath = str(pathlib.Path.home())

        #: (:obj:`str`) beamtime file prefix
        self.__bt_prefix = "beamtime-metadata-"
        #: (:obj:`str`) beamtime file postfix
        self.__bt_postfix = ".json"

        #: (:obj:`str`) scicat dataset file pattern
        self.__ds_pattern = "scicat-datasets-{beamtimeid}.lst"
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__ids_pattern = "scicat-ingested-datasets-{beamtimeid}.lst"
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__hostname = socket.gethostname()

        #: (:obj:`str`) ingestor log directory
        self.__var_dir = ""

        #: (:obj:`list` <:obj:`str`>) scandir blacklist
        self.__scandir_blacklist = [
            "/gpfs/current/scratch_bl",
            "/gpfs/current/processed",
            "/gpfs/current/shared"
        ]
        if "scandir_blacklist" in self.__config.keys() \
           and isinstance(self.__config["scandir_blacklist"], list):
            self.__scandir_blacklist = []
            for sdir in self.__config["scandir_blacklist"]:
                if sdir:
                    self.__scandir_blacklist.append(sdir.format(
                        homepath=self.__homepath))

        #: (:obj:`list` <:obj:`str`>) beamtime type blacklist
        self.__beamtime_type_blacklist = ['P']

        if "beamtime_type_blacklist" in self.__config.keys() \
           and isinstance(self.__config["beamtime_type_blacklist"], list):
            self.__beamtime_type_blacklist = []
            for btype in self.__config["beamtime_type_blacklist"]:
                if btype:
                    self.__beamtime_type_blacklist.append(btype)

        #: (:obj:`list` <:obj:`str`>) beamtime type blacklist
        self.__beamtimeid_blacklist = []

        #: (:obj:`list` <:obj:`str`>) beamtime id blacklist file
        self.__beamtimeid_blacklist_file = ""

        if "beamtimeid_blacklist_file" in self.__config.keys():
            self.__beamtimeid_blacklist_file = \
                self.__config["beamtimeid_blacklist_file"].format(
                    homepath=self.__homepath)

            self._update_beamtimeid_blacklist()

        #: (:obj:`bool`) access groups from proposals
        self.__groups_from_proposal = False
        if "owner_access_groups_from_proposal" in self.__config.keys():
            self.__groups_from_proposal = \
                self.__config["owner_access_groups_from_proposal"]

        if "beamtime_dirs" in self.__config.keys() \
           and isinstance(self.__config["beamtime_dirs"], list):
            self.__beamtime_dirs = []
            for bdir in self.__config["beamtime_dirs"]:
                if bdir:
                    self.__beamtime_dirs.append(bdir.format(
                        homepath=self.__homepath))

        if "ingestor_var_dir" in self.__config.keys():
            self.__var_dir = self.__config["ingestor_var_dir"]
        if self.__var_dir == "/":
            self.__var_dir = ""

        if "beamtime_filename_prefix" in self.__config.keys():
            self.__bt_prefix = self.__config["beamtime_filename_prefix"]

        if "beamtime_filename_postfix" in self.__config.keys():
            self.__bt_postfix = self.__config["beamtime_filename_postfix"]

        if "datasets_filename_pattern" in self.__config.keys():
            self.__ds_pattern = self.__config["datasets_filename_pattern"]

        if "ingested_datasets_filename_pattern" in self.__config.keys():
            self.__ids_pattern = \
                self.__config["ingested_datasets_filename_pattern"]

        if not self.__beamtime_dirs:
            get_logger().warning(
                'DatasetIngest: Beamtime directories not defined')

    def start(self):
        """ start ingestion """

        for path in self.__beamtime_dirs:
            get_logger().info("DatasetIngest: beamtime path: %s" % str(path))
            files = self._find_bt_files(
                path, self.__bt_prefix, self.__bt_postfix)

            for bt in files:
                get_logger().info("DatasetIngest: beamtime file: %s" % str(bt))

                ffn = os.path.abspath(os.path.join(path, bt))
                try:
                    try:
                        with open(ffn) as fl:
                            btmd = json.loads(fl.read())
                    except Exception:
                        time.sleep(0.1)
                        with open(ffn) as fl:
                            btmd = json.loads(fl.read())
                    if self._check_beamtime_not_in_blacklist(btmd):
                        self._ingest_scandir(path, btmd, ffn)
                except Exception as e:
                    get_logger().warning(
                        "%s cannot be ingested: %s" % (ffn, str(e)))

    def _update_beamtimeid_blacklist(self):
        """ update beamtime meta
        """
        if self.__beamtimeid_blacklist_file and \
           os.path.isfile(self.__beamtimeid_blacklist_file):
            with open(self.__beamtimeid_blacklist_file) as fl:
                btids = fl.read().strip().split("\n")
            self.__beamtimeid_blacklist.extend(
                [str(btid).strip() for btid in btids])

    def _check_beamtime_not_in_blacklist(self, meta):
        """ check if beamtime is not in blacklist

        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :returns: flag if beamtime not in blacklist
        :rtype: :obj:`bool`
        """
        if self.__beamtime_type_blacklist:
            proposalType = meta.get("proposalType", None)
            if proposalType and proposalType in self.__beamtime_type_blacklist:
                return False
        if self.__beamtimeid_blacklist:
            beamtimeId = meta.get("beamtimeId", None)
            if beamtimeId and beamtimeId in self.__beamtimeid_blacklist:
                return False
        return True

    def _ingest_scandir(self, path, meta, beamtimefile):
        """ constructor

        :param path: scan dir path
        :type path: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param beamtimefile: beamtime file
        :type beamtimefile: :obj:`str`
        """
        #: (:obj:`str`) beamtime id
        beamtimeId = meta["beamtimeId"]

        #: (:obj:`str`) core path
        corepath = meta.get("corePath", None)

        #: (:obj:`str`) beamtime path
        bpath = os.path.split(beamtimefile)[0]

        conv = PathConverter(
            corepath, bpath,
            self.__usecorepath and corepath)

        path = conv.to_core(path)

        #: (:obj:`str`) datasets file name
        dslist_filename = self.__ds_pattern.format(
            beamtimeid=beamtimeId, hostname=self.__hostname)
        #: (:obj:`str`) ingescted datasets file name
        idslist_filename = self.__ids_pattern.format(
            beamtimeid=beamtimeId, hostname=self.__hostname)
        dslfiles = glob.glob(
            "%s/**/%s" % (path, dslist_filename), recursive=True)
        for fn in dslfiles:
            get_logger().info("DatasetIngest: dataset list: %s" % str(fn))
            ifn = fn[:-(len(dslist_filename))] + idslist_filename
            if self.__var_dir:
                ifn = "%s%s" % (
                    self.__var_dir.format(
                        beamtimeid=beamtimeId,
                        homepath=self.__homepath), ifn)
            ipath, _ = os.path.split(ifn)
            if not os.path.isdir(ipath):
                os.makedirs(ipath, exist_ok=True)
            scpath, pfn = os.path.split(fn)
            if conv.to_core(scpath) in self.__scandir_blacklist or \
               conv.from_core(scpath) in self.__scandir_blacklist:
                continue
            ingestor = DatasetIngestor(
                self.__config,
                scpath, fn, ifn, meta, conv.to_core(beamtimefile))
            if self.__groups_from_proposal and \
               ("accessGroups" not in meta or "ownerGroup" not in meta):
                meta = ingestor.append_proposal_groups()
            try:
                ingestor.check_list(reingest=True)
                ingestor.clear_tmpfile()
                if ingestor.waiting_datasets():
                    token = ingestor.get_token()
                    for scan in ingestor.waiting_datasets():
                        ingestor.reingest(scan, token)
                ingestor.update_from_tmpfile()
            except Exception as e:
                get_logger().warning(str(e))

    def _find_bt_files(self, path, prefix, postfix):
        """ find beamtime files with given prefix and postfix in the given path

        :param path: beamtime directory
        :type path: :obj:`str`
        :param prefix: file name prefix
        :type prefix: :obj:`str`
        :param postfix: file name postfix
        :type postfix: :obj:`str`
        :returns: list of found files
        :rtype: :obj:`list` <:obj:`str`>
        """
        files = []
        try:
            if os.path.isdir(path):
                files = [fl for fl in os.listdir(path)
                         if (fl.startswith(prefix)
                             and fl.endswith(postfix))]
        except Exception as e:
            get_logger().warning(str(e))
        return files


def main():
    """ the main program function
    """

    description = "Re-ingestion script for SciCat RawDatasets."

    epilog = "" \
        " examples:\n" \
        "      scicat_dataset_ingest -c ~/.scingestor.yaml\n " \
        "     scicat_dataset_ingest -c ~/.scingestor.yaml -l debug\n" \
        "\n"
    parser = argparse.ArgumentParser(
        description=description, epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--configuration", dest="config",
        help="configuration file name")
    parser.add_argument(
        "-l", "--log", dest="log",
        help="logging level, i.e. debug, info, warning, error, critical",
        default="info")
    parser.add_argument(
        "-f", "--log-file", dest="logfile",
        help="log file name")
    parser.add_argument(
        "-t", "--timestamps", action="store_true",
        default=False, dest="timestamps",
        help="timestamps in logs")

    options = parser.parse_args()

    init_logger("SciCatDatasetIngest", options.log,
                options.timestamps, options.logfile)

    di = DatasetIngest(options)
    di.start()
    sys.exit(0)
