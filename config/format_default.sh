#!/bin/env bash

# read parameters from the command-line
if [[ ! -z $1 ]]; then
    export SCICAT_URL="$1"
fi
if [[ ! -z $2 ]]; then
    export BEAMTIME_DIR="$2"
fi
if [[ ! -z $3 ]]; then
    export INGESTOR_VAR_DIR="$3"
fi

# default parameters
if [[ -z $SCICAT_URL ]]; then
    export SCICAT_URL="http://scicat.desy.de/api/v3"
fi
if [[ -z $BEAMTIME_DIR ]]; then
    export BEAMTIME_DIR="/gpfs/current"
fi
if [[ -z $INGESTOR_VAR_DIR ]]; then
    export INGESTOR_VAR_DIR="/gpfs/current/scratch_bl/scingestor"
fi

rm -f default.yaml default.yaml.tmp

( echo "cat <<EOF >default.yaml";
  cat default_template.yaml;
  echo "EOF";
) >default.yaml.tmp
. default.yaml.tmp

rm -f default.yaml.tmp
