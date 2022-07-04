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


class SciCatMockHandler(BaseHTTPRequestHandler):

    """ scicat mock server handler """

    def do_POST(self):
        """ implementation of action for http POST requests
        """
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Accept', 'application/json')
        self.end_headers()

        # print(self.headers)
        # print(self.path)
        length = int(self.headers.get('Content-Length'))
        contenttype = self.headers.get('Content-Type')
        in_data = self.rfile.read(length)

        message = ""

        if self.path.lower() == '/users/login' and \
           contenttype == 'application/json':
            self.server.userslogin.append(in_data)
            dt = json.loads(in_data)
            # print("Login: %s" % dt)
            print("Login: %s" % dt["username"])
            message = json.dumps(
                {"id": "H3BxDGwgvnGbp5ZlhdksDKdIpljtEm8"
                 "yilq1B7s7CygIaxbQRAMmZBgJ6JW2GjnX"})

        elif self.path.lower() == '/datasets' and \
                contenttype == 'application/json':
            self.server.datasets.append(in_data)
            dt = json.loads(in_data)
            # print("Datasets: %s" % dt)
            print("Datasets: %s" % dt["pid"])
            message = "{}"

        elif self.path.lower() == '/origdatablocks' and \
                contenttype == 'application/json':
            self.server.origdatablocks.append(in_data)
            dt = json.loads(in_data)
            print("OrigDatablocks: %s" % dt)
            message = "{}"

        else:
            self.server.others.append(in_data)
            print(in_data)

        self.wfile.write(bytes(message, "utf8"))

    def do_GET(self):
        """ implementation of action for http GET requests
        """
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        message = "SciCat mock server for tests!"
        self.wfile.write(bytes(message, "utf8"))


class SciCatTestServer(HTTPServer):

    """ scicat test server """

    stopped = False
    allow_reuse_address = True

    def __init__(self, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)

        # (:obj:`list`<:obj:`str`>) ingested datasets
        self.datasets = []
        # (:obj:`list`<:obj:`str`>) ingested origdatablocks
        self.origdatablocks = []
        # (:obj:`list`<:obj:`str`>) requested credentials
        self.userslogin = []
        # (:obj:`list`<:obj:`str`>) other ingestions
        self.others = []

    def run(self):
        try:
            self.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.server_close()


def main():
    ts = SciCatTestServer(('', 8000), SciCatMockHandler)
    ts.run()
    print("\nLogins:", ts.userslogin)
    print("Datasets:", ts.datasets)
    print("OrigDatablocks:", ts.origdatablocks)
    print("Others:", ts.others)


if __name__ == "__main__":
    main()
