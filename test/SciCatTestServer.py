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

""" SciCat  Mock Test Server """

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import uuid
import requests


class SciCatMockHandler(BaseHTTPRequestHandler):

    """ scicat mock server handler """

    def set_json_header(self, resp=200):
        """ set json header

        :param resp: response value
        :type resp: :obj:`int`
        """
        self.send_response(resp)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Accept', 'application/json')
        self.end_headers()

    def set_html_header(self, resp=200):
        """ set html header

        :param resp: response value
        :type resp: :obj:`int`
        """
        self.send_response(resp)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_PATCH(self):
        """ implementation of action for http PATCH requests
        """
        self.server.counter += 1
        if self.server.counter in self.server.error_requests:
            message = json.dumps({"Error": "Internal Error"})
            # message = json.dumps(
            #     {"Error":
            #      "Internal Error %s" % self.server.counter})
            # message = json.dumps(
            #     {"Error": "Internal Error for %s" % self.path})
            resp = 500
            self.set_json_header(resp)
            self.wfile.write(bytes(message, "utf8"))
            return

        length = int(self.headers.get('Content-Length'))
        contenttype = self.headers.get('Content-Type')
        in_data = self.rfile.read(length)

        message = ""

        if self.path.lower().startswith(
                '/datasets/') and \
                contenttype == 'application/json':
            self.server.datasets.append(in_data)
            # print(in_data)
            # print(type(in_data))
            try:
                if "?access_token=" not in self.path.lower():
                    raise Exception("Missing access_token")
                spath = self.path.lower().split("?access_token=")
                if not spath[-1]:
                    raise Exception("Empty access_token")
                dt = json.loads(in_data)
                # print("Datasets: %s" % dt)
                print("Datasets: %s" % dt["pid"])
                npid = dt["pid"]
                dt["pid"] = npid
                self.server.pid_dataset[npid] = json.dumps(dt)
                message = "{}"
                resp = 200
            except Exception as e:
                message = json.dumps({"Error": str(e)})
                resp = 400
            self.set_json_header(resp)

        else:
            self.set_json_header()
            self.server.others.append(in_data)
            print("Others: %s" % str(in_data))

        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        """ implementation of action for http POST requests
        """

        self.server.counter += 1
        if self.server.counter in self.server.error_requests:
            message = json.dumps({"Error": "Internal Error"})
            # message = json.dumps(
            #     {"Error":
            #      "Internal Error %s" % self.server.counter})
            # message = json.dumps(
            #     {"Error": "Internal Error for %s" % self.path})
            resp = 500
            self.set_json_header(resp)
            self.wfile.write(bytes(message, "utf8"))
            return

        # print(self.headers)
        # print(self.path)
        length = int(self.headers.get('Content-Length'))
        contenttype = self.headers.get('Content-Type')
        in_data = self.rfile.read(length)

        message = ""

        if self.path.lower() == '/users/login' and \
           contenttype == 'application/json':
            self.server.userslogin.append(in_data)
            try:
                dt = json.loads(in_data)
                # print("Login: %s" % dt)
                print("Login: %s" % dt["username"])
                if not dt["username"]:
                    raise Exception("Empty username")
                if not dt["password"]:
                    raise Exception("Empty password")
                message = json.dumps(
                    {"id": "H3BxDGwgvnGbp5ZlhdksDKdIpljtEm8"
                     "yilq1B7s7CygIaxbQRAMmZBgJ6JW2GjnX"})
                resp = 200
            except Exception as e:
                message = json.dumps({"Error": str(e)})
                resp = 400
            self.set_json_header(resp)

        elif self.path.lower().startswith(
                '/datasets?access_token=') and \
                contenttype == 'application/json':
            # print(in_data)
            # print(type(in_data))
            spath = self.path.lower().split("?access_token=")
            try:
                if not spath[-1]:
                    raise Exception("Empty access_token")
                self.server.datasets.append(in_data)
                dt = json.loads(in_data)
                # print("Datasets: %s" % dt)
                print("Datasets: %s" % dt["pid"])
                npid = self.server.pidprefix + dt["pid"]
                dt["pid"] = npid
                self.server.pid_dataset[npid] = json.dumps(dt)
                message = "{}"
                resp = 200
            except Exception as e:
                message = json.dumps({"Error": str(e)})
                resp = 400
            self.set_json_header(resp)

        elif self.path.lower().startswith(
                '/datasets/') and \
                contenttype == 'application/json':
            spath = self.path.lower().split("?access_token=")
            resp = 200
            try:
                if not spath[-1]:
                    raise Exception("Empty access_token")
                if len(spath) == 2:
                    lpath = spath[0].split("/")
                    if len(lpath) == 4 and lpath[3] == "attachments":
                        pid = lpath[2]
                        pid = pid.replace("%2f", "/")
                        print("Datasets Attachments: %s" % pid)
                        self.server.attachments.append((pid, in_data))
                        try:
                            dt = json.loads(in_data)
                            npid = str(uuid.uuid4())
                            dt["id"] = npid
                            self.server.id_attachment[npid] = json.dumps(dt)
                            # print("IDA POST %s" % self.server.id_attachment)
                            resp = 200
                            message = "{}"
                        except Exception as e:
                            message = json.dumps({"Error": str(e)})
                            resp = 400
            except Exception as e:
                message = json.dumps({"Error": str(e)})
                resp = 400
            self.set_json_header(resp)
        elif self.path.lower().startswith(
                '/origdatablocks?access_token=') and \
                contenttype == 'application/json':
            self.server.origdatablocks.append(in_data)
            spath = self.path.lower().split("?access_token=")
            try:
                if not spath[-1]:
                    raise Exception("Empty access_token")
                dt = json.loads(in_data)
                print("OrigDatablocks: %s" % dt['datasetId'])
                npid = str(uuid.uuid4())
                dt["id"] = npid
                self.server.id_origdatablock[npid] = json.dumps(dt)
                message = "{}"
                resp = 200
            except Exception as e:
                message = json.dumps({"Error": str(e)})
                resp = 400
            self.set_json_header(resp)
        else:
            self.set_json_header()
            self.server.others.append(in_data)
            print("Others: %s" % str(in_data))

        self.wfile.write(bytes(message, "utf8"))

    def do_GET(self):
        """ implementation of action for http GET requests
        """

        self.server.counter += 1
        if self.server.counter in self.server.error_requests:
            # message = json.dumps(
            #     {"Error":
            #      "Internal Error %s" % self.server.counter})
            message = json.dumps({"Error": "Internal Error"})
            # message = json.dumps(
            #     {"Error": "Internal Error for %s" % self.path})
            resp = 500
            self.set_html_header(resp)
            self.wfile.write(bytes(message, "utf8"))
            return

        message = "SciCat mock server for tests!"
        path = self.path
        if "?access_token=" in path:
            spath = path.split("?access_token=")
        elif "&access_token=" in path:
            spath = path.split("&access_token=")
        else:
            spath = [path]
        dspath = spath[0].split("/")
        try:
            if not spath[-1]:
                raise Exception("Empty access_token")

            if len(dspath) > 2 and dspath[1].lower() == "datasets":
                pid = dspath[2].replace("%2F", "/")
                if len(dspath) == 3:
                    if pid in self.server.pid_dataset:
                        message = self.server.pid_dataset[pid]
                    else:
                        message = ""
                elif (len(dspath) == 4 and
                      dspath[3].lower() == "origdatablocks"):
                    odbs = []
                    for odb in self.server.id_origdatablock.values():
                        jodb = json.loads(odb)
                        if "datasetId" in jodb.keys() and \
                           jodb["datasetId"] == pid:
                            odbs.append(jodb)
                    message = json.dumps(odbs)
                elif (len(dspath) == 4 and
                      dspath[3].lower() == "attachments"):
                    odbs = []
                    # print("IDA DS GET %s" % self.server.id_attachment)
                    for odb in self.server.id_attachment.values():
                        jodb = json.loads(odb)
                        if "datasetId" in jodb.keys() and \
                           jodb["datasetId"] == pid:
                            odbs.append(jodb)
                    message = json.dumps(odbs)
            elif len(dspath) > 2 and dspath[1].lower() == "proposals":
                pid = dspath[2].replace("%2F", "/")
                if len(dspath) == 3:
                    if pid in self.server.pid_proposal:
                        message = self.server.pid_proposal[pid]
                    else:
                        message = ''
            elif len(dspath) > 2 and dspath[1].lower() == "origdatablocks":
                pid = requests.utils.unquote(dspath[2])
                if len(dspath) == 3:
                    pid = dspath[2].replace("%2F", "/")
                    if pid in self.server.id_origdatablock:
                        message = self.server.id_origdatablock[pid]
                    else:
                        message = ""
            elif len(dspath) > 2 and dspath[1].lower() == "attachments":
                pid = requests.utils.unquote(dspath[2])
                if len(dspath) == 3:
                    pid = dspath[2].replace("%2F", "/")
                    # print("IDA GET %s" % self.server.id_attachment)
                    if pid in self.server.id_attachament:
                        message = self.server.id_attachament[pid]
                    else:
                        message = ""
            resp = 200
        except Exception as e:
            message = json.dumps({"Error": str(e)})
            resp = 400
        self.set_html_header(resp)
        self.wfile.write(bytes(message, "utf8"))

    def do_DELETE(self):
        """ implementation of action for http DELETE requests
        """
        self.server.counter += 1
        if self.server.counter in self.server.error_requests:
            # message = json.dumps(
            #     {"Error":
            #    "Internal Error %s" % self.server.counter})
            message = json.dumps({"Error": "Internal Error"})
            # message = json.dumps(
            #     {"Error": "Internal Error for %s" % self.path})
            resp = 500
            self.set_html_header(resp)
            self.wfile.write(bytes(message, "utf8"))
            return

        message = "SciCat mock server for tests!"
        path = self.path
        if "?access_token=" in path:
            spath = path.split("?access_token=")
        elif "&access_token=" in path:
            spath = path.split("&access_token=")
        else:
            spath = [path]
        dspath = spath[0].split("/")
        # print("PATH1", dspath)
        try:
            if not spath[-1]:
                raise Exception("Empty access_token")

            if len(dspath) > 2 and dspath[1].lower() == "datasets":
                pid = dspath[2].replace("%2F", "/")
                if len(dspath) == 3:
                    if pid in self.server.pid_dataset.keys():
                        self.server.pid_dataset.pop(pid)
                        print("Datasets: delete %s" % pid)
                elif len(dspath) == 4 and dspath[3].lower() == "attachments":
                    aid = dspath[4].replace("%2F", "/")
                    # print("IDA DELETE %s" % self.server.id_attachment)
                    if aid in self.server.id_attachment.keys():
                        dt = self.server.id_attachment.pop(aid)
                        print("Datasets Attachments: delete %s"
                              % json.loads(dt)['datasetId'])
            elif len(dspath) > 2 and dspath[1].lower() == "proposals":
                pid = dspath[2].replace("%2F", "/")
                if len(dspath) == 3:
                    if pid in self.server.pid_proposal.keys():
                        self.server.pid_proposal.pop(pid)
                        print("Proposals: delete %s" % pid)
            elif len(dspath) > 2 and dspath[1].lower() == "origdatablocks":
                pid = dspath[2].replace("%2F", "/")
                if len(dspath) == 3:
                    if pid in self.server.id_origdatablock.keys():
                        dt = self.server.id_origdatablock.pop(pid)
                        print("OrigDatablocks: delete %s"
                              % json.loads(dt)['datasetId'])
            resp = 200
        except Exception as e:
            message = json.dumps({"Error": str(e)})
            resp = 400
        self.set_html_header(resp)
        self.wfile.write(bytes(message, "utf8"))


class SciCatTestServer(HTTPServer):

    """ scicat test server """

    stopped = False
    allow_reuse_address = True

    def __init__(self, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)

        #: (:obj:`list`<:obj:`str`>) ingested datasets
        self.datasets = []
        #: (:obj:`list`<:obj:`str`>) ingested origdatablocks
        self.origdatablocks = []
        #: (:obj:`list`<:obj:`str`>) requested credentials
        self.userslogin = []
        #: (:obj:`list`<:obj:`str`>) other ingestions
        self.others = []
        #: (:obj:`list`<:obj:`str`>) ingested attachments
        self.attachments = []
        #: (:obj:`dict`<:obj:`str`, :obj:`str`>) dictionary with datasets
        self.pid_dataset = {}
        #: (:obj:`dict`<:obj:`str`, :obj:`str`>) dictionary with proposal
        self.pid_proposal = {}
        #: (:obj:`dict`<:obj:`str`, :obj:`str`>) dictionary with datablocks
        self.id_origdatablock = {}
        #: (:obj:`dict`<:obj:`str`, :obj:`str`>) dictionary with attachements
        self.id_attachment = {}
        #: (:obj:`int`) id counter
        self.counter = 0
        #: (:obj:`int`) request ids with error
        self.error_requests = []
        #: (:obj:`str`) pid prefix
        self.pidprefix = ""
        # self.pidprefix = "10.3204/"

    def reset(self):
        self.datasets = []
        self.origdatablocks = []
        self.userslogin = []
        self.attachments = []
        self.others = []
        self.pid_dataset = {}
        self.pid_proposal = {}
        self.id_origdatablock = {}
        self.id_attachment = {}
        self.counter = 0
        self.error_requests = []

    def run(self):
        try:
            self.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.server_close()


def main():
    ts = SciCatTestServer(('', 8881), SciCatMockHandler)
    ts.run()
    print("\nLogins:", ts.userslogin)
    print("Datasets:", ts.datasets)
    print("OrigDatablocks:", ts.origdatablocks)
    print("Others:", ts.others)


if __name__ == "__main__":
    main()
