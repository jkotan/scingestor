#!/usr/bin/env bash


echo "install scicat-dataset-ingestor"
docker exec  --user root ndts chown -R scuser:scuser .
docker exec ndts python3 setup.py build
if [ "$?" != "0" ]; then exit 255; fi
docker exec  --user root ndts python3 setup.py install
if [ "$?" != "0" ]; then exit 255; fi
