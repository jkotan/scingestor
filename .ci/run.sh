#!/usr/bin/env bash

echo "run scicat-dataset-ingestor tests"
docker exec ndts  python3 -m pytest --cov=scingestor --cov-report=term-missing test
if [ "$?" != "0" ]; then exit 255; fi
