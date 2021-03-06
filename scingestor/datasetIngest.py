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

from .configuration import load_config
from .datasetIngestor import DatasetIngestor
from .logger import get_logger, init_logger


class DatasetIngest:

    """ Dataset Ingest command
    """

    def __init__(self, options):
        """ constructor

        :param options: time delay
        :type options: :obj:`str`
        """
        # (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = {}
        if options.config:
            self.__config = load_config(options.config) or {}
            get_logger().debug("CONFIGURATION: %s" % str(self.__config))

        # (:obj:`list` <:obj:`str`>) beamtime directories
        self.beamtime_dirs = [
            # "/gpfs/current",
            # "/gpfs/commissioning",
        ]
        if "beamtime_dirs" in self.__config.keys() \
           and isinstance(self.__config["beamtime_dirs"], list):
            self.beamtime_dirs = self.__config["beamtime_dirs"]

        # (:obj:`str`) beamtime file prefix
        self.bt_prefix = "beamtime-metadata-"
        # (:obj:`str`) beamtime file postfix
        self.bt_postfix = ".json"

        # (:obj:`float`) timeout value for inotifyx get events
        self.timeout = 0.1

        if not self.beamtime_dirs:
            get_logger().warning(
                'DatasetIngest: Beamtime directories not defined')

    def start(self):
        """ start ingestion """

        for path in self.beamtime_dirs:
            get_logger().info("DatasetIngest: beamtime path: %s" % str(path))
            files = self.find_bt_files(
                path, self.bt_prefix, self.bt_postfix)

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
                    self.ingest_scandir(path, btmd, ffn)
                except Exception as e:
                    get_logger().warning(
                        "%s cannot be ingested: %s" % (ffn, str(e)))

    def ingest_scandir(self, path, meta, bpath):
        """ constructor

        :param path: scan dir path
        :type path: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`,`any`>
        :param bpath: beamtime file
        :type bpath: :obj:`str`
        """
        # # (:obj:`str`) scan dir path
        # self.__path = path
        # # (:obj:`str`) beamtime path and file name
        # self.__bpath = bpath
        # # (:obj:`dict` <:obj:`str`,`any`>) beamtime configuration
        # self.__meta = meta
        # (:obj:`str`) beamtime id
        beamtimeId = meta["beamtimeId"]
        # (:obj:`str`) beamline
        ds_pattern = "scicat-datasets-{bt}.lst"
        # (:obj:`str`) indested scicat dataset file pattern
        ids_pattern = "scicat-ingested-datasets-{bt}.lst"

        # (:obj:`str`) datasets file name
        dslist_filename = ds_pattern.format(bt=beamtimeId)
        # (:obj:`str`) ingescted datasets file name
        idslist_filename = ids_pattern.format(bt=beamtimeId)
        dslfiles = glob.glob(
            "%s/**/%s" % (path, dslist_filename), recursive=True)
        for fn in dslfiles:
            get_logger().info("DatasetIngest: dataset list: %s" % str(fn))
            ifn = fn[:-(len(dslist_filename))] + idslist_filename
            scpath, pfn = os.path.split(fn)
            ingestor = DatasetIngestor(
                self.__config,
                scpath, fn, ifn, meta, bpath, 0)
            ingestor.check_list(reingest=True)
            ingestor.clear_tmpfile()
            if ingestor.waiting_datasets():
                token = ingestor.get_token()
                for scan in ingestor.waiting_datasets():
                    ingestor.reingest(scan, token)
            ingestor.update_from_tmpfile()

    def find_bt_files(self, path, prefix, postfix):
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

    description = "SciCat Dataset ingestion"

    epilog = "" \
        " examples:\n" \
        "       scicat_dataset_ingest -l debug\n\n" \
        "       scicat_dataset_ingest -c myconfig.yaml\n" \
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
    try:
        options = parser.parse_args()
    except Exception as e:
        sys.stderr.write("Error: %s\n" % str(e))
        sys.stderr.flush()
        parser.print_help()
        print("")
        sys.exit(255)

    init_logger("SciCatDatasetIngestor", options.log)

    di = DatasetIngest(options)
    di.start()
    sys.exit(0)
