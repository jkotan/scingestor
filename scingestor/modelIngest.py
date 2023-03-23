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
import sys
import argparse
import json
import pathlib
import requests

from .configuration import load_config
from .logger import get_logger, init_logger


class ModelIngest:

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

        #: (:obj:`str`) scicat model name
        self.__scicat_model = str(options.model or "")

        #: (:obj:`str`) token file
        self.__tokenfile = options.tokenfile

        #: (:obj:`list` <:obj:`str`>) metadata files
        self.__metafiles = options.args

        #: (:obj:`str`) home directory
        self.__homepath = str(pathlib.Path.home())

        #: (:obj:`dict` <:obj:`str`, :obj:`str`>) request headers
        self.__headers = {'Content-Type': 'application/json',
                          'Accept': 'application/json'}

        #: (:obj:`str`) scicat url
        self.__scicat_url = "http://localhost:8881"

        #: (:obj:`int`) maximal counter value for post tries
        self.__maxcounter = 100

        #: (:obj:`str`) username
        self.__username = 'ingestor'

        #: (:obj:`str`) beamtime id
        self.__incd = None

        if "ingestor_username" in self.__config.keys():
            self.__username = self.__config["ingestor_username"]

        if "ingestor_credential_file" in self.__config.keys():
            with open(self.__config["ingestor_credential_file"].format(
                    homepath=self.__homepath)) as fl:
                self.__incd = fl.read().strip()

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

        if "scicat_url" in self.__config.keys():
            self.__scicat_url = self.__config["scicat_url"]

        #: (:obj:`str`) scicat users login
        self.__scicat_users_login = "Users/login"

        # self.__tokenurl = "http://www-science3d.desy.de:3000/api/v3/" \
        #       "Users/login"
        if not self.__scicat_url.endswith("/"):
            self.__scicat_url = self.__scicat_url + "/"
        #: (:obj:`str`) token url
        self.__tokenurl = self.__scicat_url + self.__scicat_users_login
        # get_logger().info(
        #     'DatasetIngestor: LOGIN %s' % self.__tokenurl)

        #: (:obj:`str`) dataset url
        self.__modelurl = self.__scicat_url + self.__scicat_model

    def start(self):
        """ start ingestion """

        if self.__tokenfile:
            with open(self.__tokenfile) as fl:
                token = fl.read().strip()
        else:
            token = self.get_token()
        for metafile in self.__metafiles:
            self._ingest_model_metadata(metafile, token)

    def _ingest_model_metadata(self, metafile, token):
        """ ingest metadata from file

        :param metafile: metadata file name
        :type metafile: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        """
        try:
            with open(metafile) as fl:
                smt = fl.read()
            get_logger().info(
                'ModelIngestor: Post the %s from %s'
                % (self.__scicat_model, metafile))
            status = self._ingest_model(smt, token)
            if status:
                return status
        except Exception as e:
            get_logger().error(
                'ModelIngestor: %s' % (str(e)))
        return None

    def _ingest_model(self, metadata, token):
        """ ingest metadata

        :param metadata: metadata in json string
        :type metadata: :obj:`str`
        :param token: ingestor token
        :type token: :obj:`str`
        :returns: rewquest startus
        :rtype: :obj:`bool`
        """
        response = requests.post(
            "%s?access_token=%s" % (self.__modelurl, token),
            headers=self.__headers,
            data=metadata)
        if not response.ok:
            raise Exception("%s" % response.text)
        return True

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
                'ModelIngestor: %s' % (str(e)))
        return ""


def main():
    """ the main program function
    """

    description = "Ingest script for SciCat Models."

    epilog = "" \
        " examples:\n" \
        "      scicat_ingest -m Samples -c ~/.scingestor.yaml \n " \
        "     scicat_ingest -m Attachments -c ~/.scingestor.yaml " \
        "-p ~/.mytoken.cfg\n" \
        "\n"
    parser = argparse.ArgumentParser(
        description=description, epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-m", "--model", dest="model",
        help="SciCat model name in plural")
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
    parser.add_argument(
        "-p", "--token-file", dest="tokenfile",
        help="file with a user token")
    parser.add_argument(
        'args', metavar='metadata_json_file', type=str,
        nargs='+', help="metadata json file(s)")

    options = parser.parse_args()

    if not options.model:
        sys.stderr.write("Error: SciCat model not define. "
                         "Use the -m or --model option\n")
        sys.stderr.flush()
        print("")
        sys.exit(255)

    init_logger("ScicatModelIngest", options.log,
                options.timestamps, options.logfile)

    di = ModelIngest(options)
    di.start()
    sys.exit(0)
