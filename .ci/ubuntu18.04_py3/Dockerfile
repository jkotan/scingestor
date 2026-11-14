FROM ubuntu:18.04

MAINTAINER DESY, Jan Kotanski <jankotan@gmail.com>

RUN apt-get -qq update && export DEBIAN_FRONTEND=noninteractive && apt-get -qq install -y software-properties-common curl
RUN curl https://repos.desy.de/pni-hdri/debian_repo.pub.gpg | apt-key add -
RUN add-apt-repository "deb https://repos.desy.de/pni-hdri/apt/debian bionic main" -y
RUN apt-get -qq update && export DEBIAN_FRONTEND=noninteractive && apt-get -qq dist-upgrade
RUN apt-get -qq update && apt-get -qq install -y  apt-utils net-tools git python3-inotifyx python3-setuptools python3-pytest python3-yaml

RUN useradd -ms /bin/bash scuser
USER scuser
WORKDIR /home/scuser
