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
import os
import glob
import json
import subprocess
import requests
import time
import enum
import socket
import pathlib

from .logger import get_logger


class UpdateStrategy(enum.Enum):

    """ Update strategy
    """
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`)
    #:       leave datasets unchanged
    NO = 0
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) patch datasets
    PATCH = 1
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) recreate datasets
    CREATE = 2
    #: (:class:`scingestor.datasetIngestor.UpdateStrategy`) patch datasets only
    #:       if scientificMetadata changed otherwise recreate datasets
    MIXED = 3


class DatasetIngestor:

    """ Dataset Ingestor
    """

    def __init__(self, configuration,
                 path, dsfile, idsfile, meta, beamtimefile):
        """ constructor

        :param configuration: dictionary with the ingestor configuration
        :type configuration: :obj:`dict` <:obj:`str`, `any`>
        :param path: scan dir path
        :type path: :obj:`str`
        :param dsfile: file with a dataset list
        :type dsfile: :obj:`str`
        :param dsfile: file with a ingester dataset list
        :type dsfile: :obj:`str`
        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param beamtimefile: beamtime filename
        :type beamtimefile: :obj:`str`
        :param pidprefix: pidprefix
        :type pidprefix: :obj:`str`
        :param ingestorcred: ingestor credential
        :type ingestorcred: :obj:`str`
        :param scicat_url: scicat_url
        :type scicat_url: :obj:`str`
        """
        #: (:obj:`dict` <:obj:`str`, `any`>) ingestor configuration
        self.__config = configuration or {}
        #: (:obj:`str`) home directory
        self.__homepath = str(pathlib.Path.home())
        #: (:obj:`str`) master file extension
        self.__ext = 'nxs'
        #: (:obj:`str`) plot file extension
        self.__plotext = 'png'
        #: (:obj:`str`) file with a dataset list
        self.__dsfile = dsfile
        #: (:obj:`str`) file with a ingested dataset list
        self.__idsfile = idsfile
        #: (:obj:`str`) file with a ingested dataset tmp list
        self.__idsfiletmp = "%s%s" % (idsfile, ".tmp")
        #: (:obj:`str`) scan path dir
        self.__path = path
        #: (:obj:`str`) metadata path dir
        self.__metapath = path
        #: (:obj:`str`) beamtime id
        self.__bid = meta["beamtimeId"]
        #: (:obj:`str`) beamline name
        self.__bl = meta["beamline"]
        #: (:obj:`str`) beamtime id
        self.__bfile = beamtimefile
        #: (:obj:`dict` <:obj:`str`, `any`>) beamtime metadata
        self.__meta = meta
        #: (:obj:`str`) indested scicat dataset file pattern
        self.__hostname = socket.gethostname()

        bpath, _ = os.path.split(beamtimefile)
        #: (:obj:`str`) relative scan path to beamtime path
        self.__relpath = os.path.relpath(path, bpath)

        #: (:obj:`str`) doi prefix
        self.__pidprefix = ""
        # self.__pidprefix = "10.3204"
        #: (:obj:`str`) username
        self.__username = 'ingestor'
        #: (:obj:`str`) update strategy
        self.__strategy = UpdateStrategy.PATCH
        #: (:obj:`str`) beamtime id
        self.__incd = None
        #: (:obj:`bool`) relative path in datablock flag
        self.__relpath_in_datablock = False
        #: (:obj:`str`) scicat url
        self.__scicat_url = "http://localhost:8881"
        #: (:obj:`str`) scicat users login
        self.__scicat_users_login = "Users/login"
        #: (:obj:`str`) scicat datasets class
        self.__scicat_datasets = "RawDatasets"
        #: (:obj:`str`) scicat proposal class
        self.__scicat_proposals = "Proposals"
        #: (:obj:`str`) scicat datablock class
        self.__scicat_datablocks = "OrigDatablocks"
        #: (:obj:`str`) scicat attachment class
        self.__scicat_attachments = "Datasets/{pid}/Attachments"
        #: (:obj:`str`) chmod string for json metadata
        self.__chmod = None
        #: (:obj:`str`) hidden attributes
        self.__hiddenattributes = None
        #: (:obj:`str`) attachment signals
        self.__attachmentsignals = None
        #: (:obj:`str`) attachment axes
        self.__attachmentaxes = None
        #: (:obj:`str`) attachment frame
        self.__attachmentframe = None
        #: (:obj:`bool`) ingest attachment flag
        self.__ingest_attachment = False
        #: (:obj:`str`) metadata copy map file
        self.__copymapfile = None
        #: (:obj:`bool`) oned metadata flag
        self.__oned = False
        #: (:obj:`int`) max oned size of metadata record
        self.__max_oned_size = None
        #: (:obj:`bool`) override attachment signals flag
        self.__override = False
        #: (:obj:`bool`) log generator command flag
        self.__logcommands = False
        #: (:obj:`bool`) empty units flag
        self.__emptyunits = True

        #: (:obj:`int`) maximal counter value for post tries
        self.__maxcounter = 100

        #: (:obj:`str`) raw dataset scan postfix
        self.__scanpostfix = ".scan.json"
        #: (:obj:`str`) origin datablock scan postfix
        self.__datablockpostfix = ".origdatablock.json"
        #: (:obj:`str`) origin datablock scan postfix
        self.__attachmentpostfix = ".attachment.json"

        #: (:obj:`str`) nexus dataset shell command
        self.__datasetcommandfile = "nxsfileinfo metadata " \
            " -o {metapath}/{scanname}{scanpostfix} " \
            " -b {beamtimefile} -p {beamtimeid}/{scanname} " \
            " -w {ownergroup}" \
            " -c {accessgroups}" \
            " {scanpath}/{scanname}.{ext}"
        #: (:obj:`str`) datablock shell command
        self.__datasetcommand = "nxsfileinfo metadata " \
            " -o {metapath}/{scanname}{scanpostfix} " \
            " -c {accessgroups}" \
            " -w {ownergroup}" \
            " -b {beamtimefile} -p {beamtimeid}/{scanname}"
        #: (:obj:`str`) datablock shell command
        self.__datablockcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*{datablockpostfix},*{scanpostfix}," \
            "*{attachmentpostfix},*~ " \
            " -p {pidprefix}/{beamtimeid}/{scanname} " \
            " -w {ownergroup}" \
            " -c {accessgroups}" \
            " -o {metapath}/{scanname}{datablockpostfix} "
        #: (:obj:`str`) datablock shell command
        self.__datablockmemcommand = "nxsfileinfo origdatablock " \
            " -s *.pyc,*{datablockpostfix},*{scanpostfix}," \
            "*{attachmentpostfix},*~ " \
            " -w {ownergroup}" \
            " -c {accessgroups}" \
            " -p {pidprefix}/{beamtimeid}/{scanname} "
        #: (:obj:`str`) datablock path postfix
        self.__datablockscanpath = " {scanpath}/{scanname} "
        #: (:obj:`str`) attachment shell command
        self.__attachmentcommand = "nxsfileinfo attachment " \
            " -w {ownergroup} -c {accessgroups} " \
            "-o {metapath}/{scanname}{attachmentpostfix} " \
            " {scanpath}/{scanname}.{plotext}"

        #: (:obj:`str`) oned generator switch
        self.__oned_switch = " --oned "
        #: (:obj:`str`) max oned size generator switch
        self.__max_oned_switch = " --max-oned-size {maxonedsize} "
        #: (:obj:`str`) copy map file generator switch
        self.__copymapfile_switch = " --copy-map-file {copymapfile} "
        #: (:obj:`str`) empty units generator switch
        self.__emptyunits_switch = " --add-empty-units "
        #: (:obj:`str`) chmod generator switch
        self.__chmod_switch = " -x {chmod} "
        #: (:obj:`str`) hidden attributes generator switch
        self.__hiddenattributes_switch = " -n {hiddenattributes} "
        #: (:obj:`str`) relpath generator switch
        self.__relpath_switch = " -r {relpath} "
        #: (:obj:`str`) attachment signals generator switch
        self.__attachmentsignals_switch = " -s {signals} "
        #: (:obj:`str`) attachment axes generator switch
        self.__attachmentaxes_switch = " -e {axes} "
        #: (:obj:`str`) attachment frame generator switch
        self.__attachmentframe_switch = " -m {frame} "
        #: (:obj:`str`) attachment override signals switch
        self.__attachmentoverride_switch = " --override "

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) request headers
        self.__headers = {'Content-Type': 'application/json',
                          'Accept': 'application/json'}

        #: (:obj:`list`<:obj:`str`>) metadata keywords without checks
        self.__withoutsm = [
            "techniques",
            "classification",
            "createdBy",
            "updatedBy",
            "datasetlifecycle",
            "numberOfFiles",
            "size",
            "createdAt",
            "updatedAt",
            "history",
            "creationTime",
            "version",
            "scientificMetadata",
            "endTime"
        ]

        #: (:obj:`list`<:obj:`str`>) ingested scan names
        self.__sc_ingested = []
        #: (:obj:`list`<:obj:`str`>) waiting scan names
        self.__sc_waiting = []
        #: (:obj:`dict`<:obj:`str`, :obj:`list`<:obj:`str`>>)
        #:   ingested scan names
        self.__sc_ingested_map = {}

        #: (:obj:`list` <:obj:`str`>) master file extension list
        self.__master_file_extension_list = ["nxs", "h5", "ndf", "nx", "fio"]

        #: (:obj:`list` <:obj:`str`>) plot file extension list
        self.__plot_file_extension_list = \
            ["png", "nxs", "h5", "ndf", "nx", "fio"]

        if "master_file_extension_list" in self.__config.keys() \
           and isinstance(self.__config["master_file_extension_list"], list):
            self.__master_file_extension_list = []
            for ext in self.__config["master_file_extension_list"]:
                if ext:
                    self.__master_file_extension_list.append(ext)
            if self.__master_file_extension_list:
                self.__ext = self.__master_file_extension_list[0]

        if "plot_file_extension_list" in self.__config.keys() \
           and isinstance(self.__config["plot_file_extension_list"], list):
            self.__plot_file_extension_list = []
            for ext in self.__config["plot_file_extension_list"]:
                if ext:
                    self.__plot_file_extension_list.append(ext)
            if self.__plot_file_extension_list:
                self.__plotext = self.__plot_file_extension_list[0]

        #: (:obj:`str`) access groups
        self.__accessgroups = \
            "{beamtimeid}-dmgt,{beamtimeid}-clbt,{beamtimeid}-part," \
            "{beamline}dmgt,{beamline}staff".format(
                beamtimeid=self.__bid, beamline=self.__bl)
        if "accessGroups" in self.__meta:
            self.__accessgroups = ",".join(self.__meta["accessGroups"])

        #: (:obj:`str`) owner group
        self.__ownergroup = \
            "{beamtimeid}-dmgt".format(
                beamtimeid=self.__bid)
        if "ownerGroup" in self.__meta:
            self.__ownergroup = self.__meta["ownerGroup"]

        #: (:obj:`bool`) metadata in log dir flag
        self.__meta_in_var_dir = False
        if "metadata_in_var_dir" in self.__config.keys():
            self.__meta_in_var_dir = self.__config["metadata_in_var_dir"]

        #: (:obj:`str`) ingestor log directory
        self.__var_dir = ""
        if "ingestor_var_dir" in self.__config.keys():
            self.__var_dir = str(
                self.__config["ingestor_var_dir"]).format(
                    beamtimeid=self.__bid,
                    homepath=self.__homepath)
        if self.__var_dir == "/":
            self.__var_dir = ""
        if self.__meta_in_var_dir and self.__var_dir:
            self.__metapath = "%s%s" % (self.__var_dir, self.__metapath)
            if not os.path.isdir(self.__metapath):
                os.makedirs(self.__metapath, exist_ok=True)

        if "dataset_pid_prefix" in self.__config.keys():
            self.__pidprefix = self.__config["dataset_pid_prefix"]
        if "ingestor_credential_file" in self.__config.keys():
            with open(self.__config["ingestor_credential_file"].format(
                    homepath=self.__homepath)) as fl:
                self.__incd = fl.read().strip()
        if "ingestor_username" in self.__config.keys():
            self.__username = self.__config["ingestor_username"]
        if "dataset_update_strategy" in self.__config.keys():
            try:
                self.__strategy = UpdateStrategy[
                    str(self.__config["dataset_update_strategy"]).upper()]
            except Exception as e:
                get_logger().warning(
                    'Wrong UpdateStrategy value: %s' % str(e))

        if "scicat_url" in self.__config.keys():
            self.__scicat_url = self.__config["scicat_url"]
        if "scicat_datasets_path" in self.__config.keys():
            self.__scicat_datasets = self.__config["scicat_datasets_path"]
        if "scicat_proposals_path" in self.__config.keys():
            self.__scicat_proposals = self.__config["scicat_proposals_path"]
        if "scicat_datablocks_path" in self.__config.keys():
            self.__scicat_datablocks = self.__config["scicat_datablocks_path"]
        if "scicat_attachments_path" in self.__config.keys():
            self.__scicat_attachments = \
                self.__config["scicat_attachments_path"]
        if "scicat_users_login_path" in self.__config.keys():
            self.__scicat_users_login = \
                self.__config["scicat_users_login_path"]

        if "relative_path_in_datablock" in self.__config.keys():
            self.__relpath_in_datablock = \
                self.__config["relative_path_in_datablock"]
        if "chmod_json_files" in self.__config.keys():
            self.__chmod = self.__config["chmod_json_files"]
        if "hidden_attributes" in self.__config.keys():
            self.__hiddenattributes = self.__config["hidden_attributes"]
        if "attachment_signal_names" in self.__config.keys():
            self.__attachmentsignals = self.__config["attachment_signal_names"]
        if "attachment_axes_names" in self.__config.keys():
            self.__attachmentaxes = self.__config["attachment_axes_names"]
        if "attachment_image_frame_number" in self.__config.keys():
            self.__attachmentframe = \
                self.__config["attachment_image_frame_number"]
        if "metadata_copy_map_file" in self.__config.keys():
            self.__copymapfile = \
                self.__config["metadata_copy_map_file"].format(
                    homepath=self.__homepath)
        if "oned_in_metadata" in self.__config.keys():
            self.__oned = self.__config["oned_in_metadata"]
        if "max_oned_size" in self.__config.keys():
            self.__max_oned_size = self.__config["max_oned_size"]
        if "override_attachment_signals" in self.__config.keys():
            self.__override = self.__config["override_attachment_signals"]
        if "log_generator_commands" in self.__config.keys():
            self.__logcommands = self.__config["log_generator_commands"]
        if "ingest_dataset_attachment" in self.__config.keys():
            self.__ingest_attachment = \
                self.__config["ingest_dataset_attachment"]
        if "add_empty_units" in self.__config.keys():
            self.__emptyunits = self.__config["add_empty_units"]

        if "scan_metadata_postfix" in self.__config.keys():
            self.__scanpostfix = self.__config["scan_metadata_postfix"]
        if "datablock_metadata_postfix" in self.__config.keys():
            self.__datablockpostfix = \
                self.__config["datablock_metadata_postfix"]
        if "attachment_metadata_postfix" in self.__config.keys():
            self.__attachmentpostfix = \
                self.__config["attachment_metadata_postfix"]

        if "file_dataset_metadata_generator" in self.__config.keys():
            self.__datasetcommandfile = \
                self.__config["file_dataset_metadata_generator"]
        if "dataset_metadata_generator" in self.__config.keys():
            self.__datasetcommand = \
                self.__config["dataset_metadata_generator"]
        if "datablock_metadata_generator" in self.__config.keys():
            self.__datablockcommand = \
                self.__config["datablock_metadata_generator"]
        if "datablock_metadata_stream_generator" in self.__config.keys():
            self.__datablockmemcommand = \
                self.__config["datablock_metadata_stream_generator"]
        if "datablock_metadata_generator_scanpath_postfix" \
           in self.__config.keys():
            self.__datablockscanpath = \
                self.__config["datablock_metadata_generator_scanpath_postfix"]
        if "attachment_metadata_generator" in self.__config.keys():
            self.__attachmentcommand = \
                self.__config["attachment_metadata_generator"]

        if "chmod_generator_switch" in self.__config.keys():
            self.__chmod_switch = \
                self.__config["chmod_generator_switch"]

        if "hidden_attributes_generator_switch" in self.__config.keys():
            self.__hiddenattributes_switch = \
                self.__config["hidden_attributes_generator_switch"]

        if "attachment_signals_generator_switch" in self.__config.keys():
            self.__attachmentsignals_switch = \
                self.__config["attachment_signals_generator_switch"]

        if "attachment_axes_generator_switch" in self.__config.keys():
            self.__attachmentaxes_switch = \
                self.__config["attachment_axes_generator_switch"]

        if "attachment_frame_generator_switch" in self.__config.keys():
            self.__attachmentframe_switch = \
                self.__config["attachment_frame_generator_switch"]

        if "metadata_copy_map_file_generator_switch" in self.__config.keys():
            self.__copymapfile_switch = \
                self.__config["metadata_copy_map_file_generator_switch"]

        if "relative_path_generator_switch" in self.__config.keys():
            self.__relpath_switch = \
                self.__config["relative_path_generator_switch"]

        if "oned_dataset_generator_switch" in self.__config.keys():
            self.__oned_switch = \
                self.__config["oned_dataset_generator_switch"]

        if "max_oned_dataset_generator_switch" in self.__config.keys():
            self.__max_oned_switch = \
                self.__config["max_oned_dataset_generator_switch"]

        if "override_attachment_signals_generator_switch" \
                in self.__config.keys():
            self.__attachmentoverride_switch = \
                self.__config["override_attachment_signals_generator_switch"]

        if "add_empty_units_generator_switch" in self.__config.keys():
            self.__emptyunits_switch = \
                self.__config["add_empty_units_generator_switch"]

        if self.__relpath_in_datablock:
            if "datablock_metadata_generator" not in self.__config.keys():
                self.__datablockcommand = \
                    self.__datablockcommand + self.__relpath_switch
            if "datablock_metadata_stream_generator" \
               not in self.__config.keys():
                self.__datablockmemcommand = \
                    self.__datablockmemcommand + self.__relpath_switch
        else:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__relpath_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__relpath_switch

        if self.__chmod is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__chmod_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__chmod_switch
            if "datablock_metadata_generator" not in self.__config.keys():
                self.__datablockcommand = \
                    self.__datablockcommand + self.__chmod_switch
            if "datablock_metadata_stream_generator" \
               not in self.__config.keys():
                self.__datablockmemcommand = \
                    self.__datablockmemcommand + self.__chmod_switch
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__chmod_switch

        if self.__hiddenattributes is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__hiddenattributes_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__hiddenattributes_switch
        if self.__copymapfile is not None:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__copymapfile_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__copymapfile_switch
        if self.__oned:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__oned_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__oned_switch

        if self.__oned and self.__max_oned_size:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__max_oned_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__max_oned_switch

        if self.__emptyunits:
            if "dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommand = \
                    self.__datasetcommand + self.__emptyunits_switch
            if "file_dataset_metadata_generator" not in self.__config.keys():
                self.__datasetcommandfile = \
                    self.__datasetcommandfile + self.__emptyunits_switch

        if self.__attachmentsignals is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentsignals_switch

        if self.__attachmentaxes is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentaxes_switch

        if self.__attachmentframe is not None:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentframe_switch

        if self.__override:
            if "attachment_metadata_generator" not in self.__config.keys():
                self.__attachmentcommand = \
                    self.__attachmentcommand + self.__attachmentoverride_switch

        if "max_request_tries_number" in self.__config.keys():
            try:
                self.__maxcounter = int(
                    self.__config["max_request_tries_number"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "request_headers" in self.__config.keys():
            try:
                self.__headers = dict(
                    self.__config["request_headers"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        if "metadata_keywords_without_checks" in self.__config.keys():
            try:
                self.__withoutsm = list(
                    self.__config["metadata_keywords_without_checks"])
            except Exception as e:
                get_logger().warning('%s' % (str(e)))

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) command format parameters
        self.__dctfmt = {
            "scanname": None,
            "chmod": self.__chmod,
            "hiddenattributes": self.__hiddenattributes,
            "copymapfile": self.__copymapfile,
            "scanpath": self.__path,
            "metapath": self.__metapath,
            "relpath": self.__relpath,
            "beamtimeid": self.__bid,
            "beamline": self.__bl,
            "pidprefix": self.__pidprefix,
            "beamtimefile": self.__bfile,
            "scanpostfix": self.__scanpostfix,
            "datablockpostfix": self.__datablockpostfix,
            "attachmentpostfix": self.__attachmentpostfix,
            "ownergroup": self.__ownergroup,
            "accessgroups": self.__accessgroups,
            "hostname": self.__hostname,
            "homepath": self.__homepath,
            "ext": self.__ext,
            "plotext": self.__plotext,
            "signals": self.__attachmentsignals,
            "axes": self.__attachmentaxes,
            "frame": self.__attachmentframe,
            "maxonedsize": self.__max_oned_size,
        }

        get_logger().debug(
            'DatasetIngestor: Parameters: %s' % str(self.__dctfmt))

        # self.__tokenurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #       "Users/login"
        if not self.__scicat_url.endswith("/"):
            self.__scicat_url = self.__scicat_url + "/"
        #: (:obj:`str`) token url
        self.__tokenurl = self.__scicat_url + self.__scicat_users_login
        # get_logger().info(
        #     'DatasetIngestor: LOGIN %s' % self.__tokenurl)

        #: (:obj:`str`) dataset url
        self.__dataseturl = self.__scicat_url + self.__scicat_datasets
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "RawDatasets"
        #: (:obj:`str`) dataset url
        self.__proposalurl = self.__scicat_url + self.__scicat_proposals
        # self.__proposalurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #    "Proposals"

        #: (:obj:`str`) origdatablock url
        self.__datablockurl = self.__scicat_url + self.__scicat_datablocks
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "OrigDatablocks"
        #: (:obj:`str`) origdatablock url

        #: (:obj:`str`) attachement url
        self.__attachmenturl = self.__scicat_url + self.__scicat_attachments
        # self.__dataseturl = "http://www-science3d.desy.de:3000/api/v3/" \
        #     "Datasets/{pid}/Attachments"
        #: (:obj:`str`) origdatablock url

    def _generate_rawdataset_metadata(self, scan):
        """ generate raw dataset metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        self.__ext = ""

        for ext in self.__master_file_extension_list:
            self.__dctfmt["ext"] = ext

            if os.path.isfile(
                    "{scanpath}/{scanname}.{ext}".format(**self.__dctfmt)):
                self.__ext = ext
                break
        self.__dctfmt["ext"] = self.__ext

        if self.__ext:
            get_logger().info(
                'DatasetIngestor: Generating %s metadata: %s %s' % (
                    self.__ext, scan,
                    "{metapath}/{scanname}{scanpostfix}".format(
                        **self.__dctfmt)))
            command = self.__datasetcommandfile.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating dataset command: %s ' % (
                        command))
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating dataset command: %s ' % (
                        command))
            subprocess.run(command, shell=True, check=True)
        else:
            get_logger().info(
                'DatasetIngestor: Generating metadata: %s %s' % (
                    scan,
                    "{metapath}/{scanname}{scanpostfix}".format(
                        **self.__dctfmt)))
            command = self.__datasetcommand.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating dataset command: %s'
                    % (command))
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating dataset command: %s'
                    % (command))
            subprocess.run(command, shell=True, check=True)

        rdss = glob.glob(
            "{metapath}/{scanname}{scanpostfix}".format(**self.__dctfmt))
        if rdss and rdss[0]:
            return rdss[0]

        return ""

    def _generate_origdatablock_metadata(self, scan):
        """ generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Generating origdatablock metadata: %s %s' % (
                scan,
                "{metapath}/{scanname}{datablockpostfix}".format(
                    **self.__dctfmt)))
        cmd = self.__datablockcommand.format(**self.__dctfmt)
        sscan = (scan or "").split(" ")
        for sc in sscan:
            cmd += self.__datablockscanpath.format(
                scanpath=self.__dctfmt["scanpath"], scanname=sc)
        if self.__logcommands:
            get_logger().info(
                'DatasetIngestor: Generating origdatablock command: %s' % cmd)
        else:
            get_logger().debug(
                'DatasetIngestor: Generating origdatablock command: %s' % cmd)
        subprocess.run(cmd, shell=True, check=True)
        odbs = glob.glob(
            "{metapath}/{scanname}{datablockpostfix}".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _generate_attachment_metadata(self, scan):
        """ generate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        self.__plotext = ""

        for ext in self.__plot_file_extension_list:
            self.__dctfmt["plotext"] = ext

            if os.path.isfile(
                    "{scanpath}/{scanname}.{plotext}".format(**self.__dctfmt)):
                self.__plotext = ext
                break
        self.__dctfmt["plotext"] = self.__plotext

        if self.__dctfmt["plotext"]:
            get_logger().info(
                'DatasetIngestor: Generating attachment metadata: %s %s' % (
                    scan,
                    "{metapath}/{scanname}{attachmentpostfix}".format(
                        **self.__dctfmt)))
            cmd = self.__attachmentcommand.format(**self.__dctfmt)
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating attachment command: %s' % cmd)
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating attachment command: %s' % cmd)
            subprocess.run(cmd, shell=True, check=True)
            adss = glob.glob(
                "{metapath}/{scanname}{attachmentpostfix}".format(
                    **self.__dctfmt))
            if adss and adss[0]:
                return adss[0]
        return ""

    def _regenerate_origdatablock_metadata(self, scan, force=False):
        """regenerate origdatablock metadata

        :param scan: scan name
        :type scan: :obj:`str`
        :param force: force flag
        :type force: :obj:`bool`
        :returns: a file name of generate file
        :rtype: :obj:`str`
        """
        mfilename = "{metapath}/{scanname}{datablockpostfix}".format(
            **self.__dctfmt)
        get_logger().info(
            'DatasetIngestor: Checking origdatablock metadata: %s %s' % (
                scan, mfilename))

        cmd = self.__datablockcommand.format(**self.__dctfmt)
        sscan = (scan or "").split(" ")
        if self.__datablockscanpath:
            dctfmt = dict(self.__dctfmt)
            for sc in sscan:
                dctfmt["scanname"] = sc
                cmd += self.__datablockscanpath.format(**dctfmt)
        get_logger().debug(
            'DatasetIngestor: Checking origdatablock command: %s ' % cmd)
        dmeta = None
        try:
            with open(mfilename, "r") as mf:
                meta = mf.read()
                dmeta = json.loads(meta)
        except Exception as e:
            get_logger().warning('%s: %s' % (scan, str(e)))
        if dmeta is None:
            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating origdatablock command: %s'
                    % cmd)
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating origdatablock command: %s'
                    % cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            cmd = self.__datablockmemcommand.format(**self.__dctfmt)
            sscan = (scan or "").split(" ")
            if self.__datablockscanpath:
                dctfmt = dict(self.__dctfmt)
                for sc in sscan:
                    dctfmt["scanname"] = sc
                    cmd += self.__datablockscanpath.format(**dctfmt)

            if self.__logcommands:
                get_logger().info(
                    'DatasetIngestor: Generating origdatablock command: %s'
                    % cmd)
            else:
                get_logger().debug(
                    'DatasetIngestor: Generating origdatablock command: %s'
                    % cmd)
            result = subprocess.run(
                cmd, shell=True,
                text=True, capture_output=True, check=True)
            nwmeta = str(result.stdout)
            try:
                dnwmeta = json.loads(nwmeta)
            except Exception as e:
                get_logger().warning('%s: %s' % (scan, str(e)))
                dnwmeta = None
            # print("M2", dnwmeta)
            if dnwmeta is not None:
                if not self._metadataEqual(dmeta, dnwmeta) or force:
                    get_logger().info(
                        'DatasetIngestor: '
                        'Generating origdatablock metadata: %s %s' % (
                            scan,
                            "{metapath}/{scanname}{datablockpostfix}".format(
                                **self.__dctfmt)))
                    with open(mfilename, "w") as mf:
                        mf.write(nwmeta)

        odbs = glob.glob(
            "{metapath}/{scanname}{datablockpostfix}".format(
                    **self.__dctfmt))
        if odbs and odbs[0]:
            return odbs[0]
        return ""

    def _metadataEqual(self, dct, dct2, skip=None, parent=None):
        """ compare two dictionaries if metdatdata is equal

        :param dct: first metadata dictionary
        :type dct: :obj:`dct` <:obj:`str`, `any`>
        :param dct2: second metadata dictionary
        :type dct2: :obj:`dct` <:obj:`str`, `any`>
        :param skip: a list of keywords to skip
        :type skip: :obj:`list` <:obj:`str`>
        :param parent: the parent metadata dictionary to use in recursion
        :type parent: :obj:`dct` <:obj:`str`, `any`>
        """
        parent = parent or ""
        w1 = [("%s.%s" % (parent, k) if parent else k)
              for k in dct.keys()
              if (not skip or
                  (("%s.%s" % (parent, k) if parent else k)
                   not in skip))]
        w2 = [("%s.%s" % (parent, k) if parent else k)
              for k in dct2.keys()
              if (not skip or
                  (("%s.%s" % (parent, k) if parent else k)
                   not in skip))]
        if len(w1) != len(w2):
            get_logger().debug(
                'DatasetIngestor: %s != %s' % (
                    list(w1), list(w2)))
            return False
        status = True
        for k, v in dct.items():
            if parent:
                node = "%s.%s" % (parent, k)
            else:
                node = k

            if not skip or node not in skip:

                if k not in dct2.keys():
                    get_logger().debug(
                        'DatasetIngestor: %s not in %s'
                        % (k,  dct2.keys()))
                    status = False
                    break
                if isinstance(v, dict):
                    if not self._metadataEqual(v, dct2[k], skip, node):
                        status = False
                        break
                else:
                    if v != dct2[k]:
                        get_logger().debug(
                            'DatasetIngestor %s: %s != %s'
                            % (k, v,  dct2[k]))

                        status = False
                        break
        return status

    def get_token(self):
        """ provides ingestor token

        :returns: ingestor token
        :rtype: :obj:`str`
        """
        try:
            response = requests.post(
                self.__tokenurl, headers=self.__headers,
                json={"username": self.__username, "password": self.__incd})
            if response.ok:
                return json.loads(response.content)["id"]
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def append_proposal_groups(self):
        """ appends owner and access groups to beamtime

        :param meta: beamtime configuration
        :type meta: :obj:`dict` <:obj:`str`, `any`>
        :param path: base file path
        :type path: :obj:`str`
        :returns: updated beamtime configuration
        :rtype: :obj:`dict` <:obj:`str`, `any`>
        """
        token = self.get_token()
        bid = self.__meta["beamtimeId"]
        try:
            resexists = requests.get(
                "{url}/{pid}/exists?access_token={token}"
                .format(
                    url=self.__proposalurl,
                    pid=bid.replace("/", "%2F"),
                    token=token))

            if resexists.ok:
                pexists = json.loads(resexists.content)["exists"]
            else:
                raise Exception("Proposal %s: %s" % (bid, resexists.text))
            if pexists:
                resget = requests.get(
                    "{url}/{pid}?access_token={token}"
                    .format(
                        url=self.__proposalurl,
                        pid=bid.replace("/", "%2F"),
                        token=token))
                if resget.ok:
                    proposal = json.loads(resget.content)
                    if "ownerGroup" not in self.__meta and \
                       "ownerGroup" in proposal:
                        self.__meta["ownerGroup"] = proposal["ownerGroup"]
                        self.__ownergroup = self.__meta["ownerGroup"]
                        self.__dctfmt["ownergroup"] = self.__ownergroup

                    if "accessGroups" not in self.__meta and \
                       "accessGroups" in proposal:
                        self.__meta["accessGroups"] = list(
                            proposal["accessGroups"])
                        self.__accessgroups = \
                            ",".join(self.__meta["accessGroups"])
                        self.__dctfmt["accessgroups"] = self.__accessgroups
                else:
                    raise Exception(
                        "Proposal %s: %s" % (bid, resget.text))
            else:
                raise Exception("Proposal %s: %s" % (bid, resexists.text))
        except Exception as e:
            get_logger().warning('%s' % (str(e)))
        return self.__meta

    def _post_dataset(self, mdic, token, mdct):
        """ post dataset

        :param mdic: metadata in dct
        :type mdic: :obj:`dct` <:obj:`str`, `any`>
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        # create a new dataset since
        # core metadata of dataset were changed
        # find a new pid
        pexist = True
        npid = mdic["pid"]
        ipid = mdct["pid"]
        while pexist:
            spid = npid.split("/")
            if len(spid) > 3:
                try:
                    ver = int(spid[-1])
                    spid[-1] = str(ver + 1)
                except Exception:
                    spid.append("2")
            else:
                spid.append("2")
            npid = "/".join(spid)
            if len(spid) > 0:
                ipid = "/".join(spid[1:])
            resexists = requests.get(
                "{url}/{pid}/exists?access_token={token}"
                .format(
                    url=self.__dataseturl,
                    pid=npid.replace("/", "%2F"),
                    token=token))
            if resexists.ok:
                pexist = json.loads(
                    resexists.content)["exists"]
            else:
                raise Exception("%s" % resexists.text)

        mdic["pid"] = ipid
        nmeta = json.dumps(mdic)
        get_logger().info(
            'DatasetIngestor: '
            'Post the dataset with a new pid: %s' % (npid))

        # post the dataset with the new pid
        response = requests.post(
            "%s?access_token=%s"
            % (self.__dataseturl, token),
            headers=self.__headers,
            data=nmeta)
        if response.ok:
            return mdic["pid"]
        else:
            raise Exception("%s" % response.text)

    def _patch_dataset(self, nmeta, pid, token, mdct):
        """ post dataset

        :param nmeta: metadata in json string
        :type nmeta: :obj:`str`
        :param pid: dataset pid
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: '
            'Patch scientificMetadata of dataset:'
            ' %s' % (pid))
        response = requests.patch(
            "{url}/{pid}?access_token={token}"
            .format(
                url=self.__dataseturl,
                pid=pid.replace("/", "%2F"),
                token=token),
            headers=self.__headers,
            data=nmeta)
        if response.ok:
            return mdct["pid"]
        else:
            raise Exception("%s" % response.text)

    def _ingest_dataset(self, metadata, token, mdct):
        """ ingests dataset

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :param mdct: metadata in dct
        :type mdct: :obj:`dct` <:obj:`str`, `any`>
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        try:
            pid = "%s/%s" % (self.__pidprefix, mdct["pid"])
            # check if dataset with the pid exists
            get_logger().info(
                'DatasetIngestor: Check if dataset exists: %s' % (pid))
            checking = True
            counter = 0
            while checking:
                resexists = requests.get(
                    "{url}/{pid}/exists?access_token={token}".format(
                        url=self.__dataseturl,
                        pid=pid.replace("/", "%2F"),
                        token=token))
                if hasattr(resexists, "content"):
                    try:
                        json.loads(resexists.content)
                        checking = False
                    except Exception:
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)
                if counter == self.__maxcounter:
                    checking = False
                counter += 1
            if resexists.ok and hasattr(resexists, "content"):
                try:
                    exists = json.loads(resexists.content)["exists"]
                except Exception:
                    exists = False
                if not exists:
                    # post the new dataset since it does not exist
                    get_logger().info(
                        'DatasetIngestor: Post the dataset: %s' % (pid))
                    response = requests.post(
                        "%s?access_token=%s" % (self.__dataseturl, token),
                        headers=self.__headers,
                        data=metadata)
                    if response.ok:
                        return mdct["pid"]
                    else:
                        raise Exception("%s" % response.text)
                elif self.__strategy != UpdateStrategy.NO:
                    # find dataset by pid
                    get_logger().info(
                        'DatasetIngestor: Find the dataset by id: %s' % (pid))
                    resds = requests.get(
                        "{url}/{pid}?access_token={token}".format(
                            url=self.__dataseturl,
                            pid=pid.replace("/", "%2F"),
                            token=token))
                    if resds.ok:
                        dsmeta = json.loads(resds.content)
                        mdic = dict(mdct)
                        mdic["pid"] = pid
                        if not self._metadataEqual(
                                dsmeta, mdic, skip=self.__withoutsm):
                            if self.__strategy in [
                                    UpdateStrategy.PATCH, UpdateStrategy.NO]:
                                nmeta = json.dumps(mdic)
                                return self._patch_dataset(
                                    nmeta, pid, token, mdct)
                            else:
                                return self._post_dataset(mdic, token, mdct)
                        else:
                            if "scientificMetadata" in dsmeta.keys() and \
                               "scientificMetadata" in mdic.keys():
                                smmeta = dsmeta["scientificMetadata"]
                                smnmeta = mdic["scientificMetadata"]
                                nmeta = json.dumps(mdic)
                                if not self._metadataEqual(smmeta, smnmeta):
                                    if self.__strategy == \
                                       UpdateStrategy.CREATE:
                                        return self._post_dataset(
                                            mdic, token, mdct)
                                    else:
                                        return self._patch_dataset(
                                            nmeta, pid, token, mdct)
                    else:
                        raise Exception("%s" % resds.text)
                else:
                    return pid
            else:
                raise Exception("%s" % resexists.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _ingest_origdatablock(self, metadata, token):
        """ ingets origdatablock

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: rewquest startus
        :rtype: :obj:`bool`
        """
        try:
            response = requests.post(
                "%s?access_token=%s" % (self.__datablockurl, token),
                headers=self.__headers,
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return False

    def _ingest_attachment(self, metadata, datasetid, token):
        """ ingets origdatablock

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: rewquest startus
        :rtype: :obj:`bool`
        """
        try:
            dsid = datasetid.replace("/", "%2F")
            url = self.__attachmenturl + "?access_token={token}"
            # get_logger().debug(
            #     'DatasetIngestor: ingest attachment %s' % (
            #         url.format(pid=dsid, token=token)))
            response = requests.post(
                url.format(pid=dsid, token=token),
                headers=self.__headers,
                data=metadata)
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return False

    def _get_id_first_origdatablock(self, datasetid, token):
        """ get id of first origdatablock with datasetid

        :param datasetid: dataset id
        :type datasetid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: origdatablock id
        :rtype: :obj:`str`
        """
        try:
            where = requests.utils.quote(
                '{"where": {"datasetId":"%s"}}'
                % datasetid.replace("/", "%2F"))
            response = requests.get(
                self.__datablockurl + "/findOne?filter=" + where
                + "&access_token=" + token,
                headers=self.__headers)
            if response.ok:
                js = response.json()
                return js['id']
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_delete_origdatablock(self, did, token):
        """ ingets origdatablock

        :param did: origdatablock id
        :type did: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        """
        try:
            response = requests.delete(
                "{url}/{pid}?access_token={token}"
                .format(
                    url=self.__datablockurl,
                    pid=did.replace("/", "%2F"),
                    token=token))
            if response.ok:
                return True
            else:
                raise Exception("%s" % response.text)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _get_pid(self, metafile):
        """ get pid from raw dataset metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :returns: dataset pid
        :rtype: :obj:`str`
        """
        pid = None
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
                pid = mt["pid"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))

        return pid

    def _ingest_rawdataset_metadata(self, metafile, token):
        """ ingest raw dataset metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if mt["proposalId"] != self.__bid:
                raise Exception(
                    "Wrong SC proposalId %s for DESY beamtimeId %s in %s"
                    % (mt["proposalId"], self.__bid, metafile))
            if not mt['pid'] or \
               not mt["pid"].startswith("%s/" % (self.__bid)):
                raise Exception(
                    "Wrong pid %s for DESY beamtimeId %s in  %s"
                    % (mt["pid"], self.__bid, metafile))
            status = self._ingest_dataset(smt, token, mt)
            if status:
                return status
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return None

    def _delete_origdatablocks(self, pid, token):
        """ delete origdatablock with given dataset pid

        :param pid: dataset id
        :type pid: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            datasetid = "%s/%s" % (self.__pidprefix, pid)
            did = self._get_id_first_origdatablock(datasetid, token)
            lastdid = [did]
            while did:
                self._get_delete_origdatablock(did, token)
                did = self._get_id_first_origdatablock(datasetid, token)
                if did in lastdid:
                    break
                lastdid.append(did)
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def _ingest_origdatablock_metadata(self, metafile, pid, token):
        """ ingest origdatablock metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param pid: dataset id
        :type pid: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if not pid or not pid.startswith(self.__bid):
                raise Exception(
                    "Wrong origdatablock datasetId %s for DESY beamtimeId "
                    "%s in  %s"
                    % (pid, self.__bid, metafile))
            if mt["datasetId"] != "%s/%s" % (self.__pidprefix, pid):
                mt["datasetId"] = "%s/%s" % (self.__pidprefix, pid)
                smt = json.dumps(mt)
                with open(metafile, "w") as mf:
                    mf.write(smt)
            status = self._ingest_origdatablock(smt, token)
            if status:
                return mt["datasetId"]
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def _ingest_attachment_metadata(self, metafile, pid, token):
        """ ingest attachment metadata

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param pid: dataset id
        :type pid: :obj:`str`
        :returns: dataset id
        :rtype: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
                mt = json.loads(smt)
            if "datasetId" in mt:
                if not pid or not pid.startswith(self.__bid):
                    raise Exception(
                        "Wrong attachment datasetId %s for DESY beamtimeId "
                        "%s in  %s"
                        % (pid, self.__bid, metafile))
                if mt["datasetId"] != "%s/%s" % (self.__pidprefix, pid):
                    mt["datasetId"] = "%s/%s" % (self.__pidprefix, pid)
                    smt = json.dumps(mt)
                    with open(metafile, "w") as mf:
                        mf.write(smt)
            dsid = "%s/%s" % (self.__pidprefix, pid)
            status = self._ingest_attachment(smt, dsid, token)
            if status:
                return dsid
        except Exception as e:
            get_logger().error(
                'DatasetIngestor: %s' % (str(e)))
        return ""

    def ingest(self, scan, token):
        """ ingest scan

        :param scan: scan name
        :type scan: :obj:`str`
        :param token: access token
        :type token: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Ingesting: %s %s' % (
                self.__dsfile, scan))

        sscan = scan.split(" ")
        self.__dctfmt["scanname"] = sscan[0] if len(sscan) > 0 else ""
        rdss = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__scanpostfix,
                metapath=self.__dctfmt["metapath"]))
        if rdss and rdss[0]:
            rds = rdss[0]
        else:
            rds = self._generate_rawdataset_metadata(self.__dctfmt["scanname"])
        mtmds = 0
        ads = None
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__datablockpostfix,
                metapath=self.__dctfmt["metapath"]))
        if odbs and odbs[0]:
            odb = odbs[0]
        else:
            odb = self._generate_origdatablock_metadata(scan)
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)
        if self.__ingest_attachment:
            adss = glob.glob(
                "{metapath}/{scan}{postfix}".format(
                    scan=self.__dctfmt["scanname"],
                    postfix=self.__attachmentpostfix,
                    metapath=self.__dctfmt["metapath"]))
            if adss and adss[0]:
                ads = adss[0]
            else:
                ads = self._generate_attachment_metadata(
                    self.__dctfmt["scanname"])
            if ads:
                mtmda = os.path.getmtime(ads)

        dbstatus = None
        dastatus = None
        pid = None
        if rds and odb:
            if rds and rds[0]:
                pid = self._ingest_rawdataset_metadata(rds, token)
            if odb and odb[0] and pid:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
            if self.__ingest_attachment and ads and ads[0] and pid:
                if pid is None and adss and adss[0]:
                    pid = self._get_pid(rdss[0])
                dastatus = self._ingest_attachment_metadata(
                    ads, pid, token)
        if pid is None:
            mtmds = 0
        if dbstatus is None:
            mtmdb = 0
        if dastatus is None:
            mtmda = 0

        sscan.extend([str(mtmds), str(mtmdb), str(mtmda)])
        self.__sc_ingested.append(sscan)
        with open(self.__idsfile, 'a+') as f:
            f.write("%s %s %s %s\n" % (scan, mtmds, mtmdb, mtmda))

    def reingest(self, scan, token):
        """ re-ingest scan

        :param scan: scan name
        :type scan: :obj:`str`
        :param token: access token
        :type token: :obj:`str`
        """
        get_logger().info(
            'DatasetIngestor: Checking: %s %s' % (
                self.__dsfile, scan))

        reingest_dataset = False
        reingest_origdatablock = False
        reingest_attachment = False
        sscan = scan.split(" ")
        self.__dctfmt["scanname"] = sscan[0] if len(sscan) > 0 else ""
        rds = None
        rdss = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__scanpostfix,
                metapath=self.__dctfmt["metapath"]))
        if rdss and rdss[0]:
            rds = rdss[0]
            mtm = os.path.getmtime(rds)
            # print(self.__sc_ingested_map.keys())
            get_logger().debug("MAP: %s" % (self.__sc_ingested_map))

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DS Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-3],
                    mtm > self.__sc_ingested_map[scan][-3]))
            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-3]:
                if self.__strategy != UpdateStrategy.NO:
                    reingest_dataset = True
        else:
            rds = self._generate_rawdataset_metadata(
                self.__dctfmt["scanname"])
            get_logger().debug("DS No File: %s True" % (scan))
            reingest_dataset = True
        mtmds = 0
        if rds:
            mtmds = os.path.getmtime(rds)

        odbs = glob.glob(
            "{metapath}/{scan}{postfix}".format(
                scan=self.__dctfmt["scanname"],
                postfix=self.__datablockpostfix,
                metapath=self.__dctfmt["metapath"]))
        if odbs and odbs[0]:
            odb = odbs[0]

            mtm0 = os.path.getmtime(odb)
            if scan not in self.__sc_ingested_map.keys() \
               or mtm0 > self.__sc_ingested_map[scan][-2]:
                reingest_origdatablock = True
            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB0 Timestamps: %s %s %s %s %s" % (
                    scan,
                    mtm0, self.__sc_ingested_map[scan][-2],
                    mtm0 - self.__sc_ingested_map[scan][-2],
                    reingest_origdatablock)
                )
            self._regenerate_origdatablock_metadata(
                scan, reingest_origdatablock)
            mtm = os.path.getmtime(odb)

            if scan in self.__sc_ingested_map.keys():
                get_logger().debug("DB Timestamps: %s %s %s %s" % (
                    scan,
                    mtm, self.__sc_ingested_map[scan][-2],
                    mtm > self.__sc_ingested_map[scan][-2]))

            if scan not in self.__sc_ingested_map.keys() \
               or mtm > self.__sc_ingested_map[scan][-2]:
                reingest_origdatablock = True
        else:
            odb = self._generate_origdatablock_metadata(scan)
            get_logger().debug("DB No File: %s True" % (scan))
            reingest_origdatablock = True
        mtmdb = 0
        if odb:
            mtmdb = os.path.getmtime(odb)

        dastatus = None
        dbstatus = None
        ads = None
        if self.__ingest_attachment:
            adss = glob.glob(
                "{metapath}/{scan}{postfix}".format(
                    scan=self.__dctfmt["scanname"],
                    postfix=self.__attachmentpostfix,
                    metapath=self.__dctfmt["metapath"]))
            if adss and adss[0]:
                ads = adss[0]
                mtm0 = os.path.getmtime(ads)
                if scan not in self.__sc_ingested_map.keys() \
                   or mtm0 > self.__sc_ingested_map[scan][-1]:
                    reingest_attachment = True
            else:
                ads = self._generate_attachment_metadata(
                    self.__dctfmt["scanname"])
                reingest_attachment = True
            if ads:
                mtm0 = os.path.getmtime(ads)

        pid = None
        if (rds and odb) or ads:
            if rds and reingest_dataset:
                pid = self._ingest_rawdataset_metadata(rds, token)
                get_logger().info(
                    "DatasetIngestor: Ingest dataset: %s" % (rds))
                oldpid = self._get_pid(rds)
                if pid and oldpid != pid:
                    # get_logger().info("PID %s %s %s" % (scan,pid,oldpid))
                    odb = self._generate_origdatablock_metadata(scan)
                    reingest_origdatablock = True
            if odb and reingest_origdatablock:
                if pid is None and rdss and rdss[0]:
                    pid = self._get_pid(rdss[0])
                self._delete_origdatablocks(pid, token)
                dbstatus = self._ingest_origdatablock_metadata(
                    odb, pid, token)
                get_logger().info(
                    "DatasetIngestor: Ingest origdatablock: %s" % (odb))

            if self.__ingest_attachment:
                if ads and reingest_attachment:
                    if pid is None and adss and adss[0]:
                        pid = self._get_pid(adss[0])
                    if not pid:
                        get_logger().error(
                            "DatasetIngestor: No dataset pid "
                            "for the attachment found: %s" % (ads))
                    else:
                        dastatus = self._ingest_attachment_metadata(
                            ads, pid, token)
                        get_logger().info(
                            "DatasetIngestor: Ingest attachment: %s" % (ads))
        mtmda = 0
        if ads:
            mtmda = os.path.getmtime(ads)

        if (pid and reingest_dataset):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmds = self.__sc_ingested_map[scan][-3]
        else:
            mtmds = 0
        if (dbstatus and reingest_origdatablock):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmdb = self.__sc_ingested_map[scan][-2]
        else:
            mtmdb = 0

        if (dastatus and reingest_attachment):
            pass
        elif scan in self.__sc_ingested_map.keys():
            mtmda = self.__sc_ingested_map[scan][-1]
        else:
            mtmda = 0

        sscan.extend([str(mtmds), str(mtmdb), str(mtmda)])
        self.__sc_ingested.append(sscan)
        with open(self.__idsfiletmp, 'a+') as f:
            f.write("%s %s %s %s\n" % (scan, mtmds, mtmdb, mtmda))

    def check_list(self, reingest=False):
        """ update waiting and ingested datasets
        """
        with open(self.__dsfile, "r") as dsf:
            scans = [sc.strip()
                     for sc in dsf.read().split("\n")
                     if sc.strip()]
        if os.path.isfile(self.__idsfile):
            with open(self.__idsfile, "r") as idsf:
                self.__sc_ingested = [
                    sc.strip().split(" ")
                    for sc in idsf.read().split("\n")
                    if sc.strip()]
        if not reingest:
            ingested = [(" ".join(sc[:-3]) if len(sc) > 3 else sc[0])
                        for sc in self.__sc_ingested]
            self.__sc_waiting = [
                sc for sc in scans if sc not in ingested]
        else:
            self.__sc_waiting = [sc for sc in scans]
            self.__sc_ingested_map = {}
            for sc in self.__sc_ingested:
                try:
                    if len(sc) > 3 and float(sc[-1]) >= 0 \
                       and float(sc[-2]) > 0 and float(sc[-3]) > 0:
                        sc[-1] = float(sc[-1])
                        sc[-2] = float(sc[-2])
                        sc[-3] = float(sc[-3])
                        self.__sc_ingested_map[" ".join(sc[:-3])] = sc
                except Exception as e:
                    get_logger().debug("%s" % str(e))

    def waiting_datasets(self):
        """ provides waitings datasets

        :returns: waitings datasets list
        :rtype: :obj:`list` <:obj:`str`>
        """
        return list(self.__sc_waiting)

    def clear_waiting_datasets(self):
        """ clear waitings datasets
        """
        self.__sc_waiting = []

    def clear_tmpfile(self):
        """ clear waitings datasets
        """
        if os.path.exists(self.__idsfiletmp):
            os.remove(self.__idsfiletmp)

    def update_from_tmpfile(self):
        """ clear waitings datasets
        """
        os.rename(self.__idsfiletmp, self.__idsfile)

    def ingested_datasets(self):
        """ provides ingested datasets

        :returns:  ingested datasets list
        :rtype: :obj:`list` <:obj:`str`>
        """
        return list(self.__sc_ingested)
