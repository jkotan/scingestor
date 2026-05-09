FROM ubuntu:26.04

# MAINTAINER DESY, Jan Kotanski <jankotan@gmail.com>

RUN sed -i 's/http:\/\/archive.ubuntu/http:\/\/de.archive.ubuntu/g' /etc/apt/sources.list.d/ubuntu.sources
RUN apt-get update
RUN apt-get install -y base-passwd  -y
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
RUN apt-get -qq update && apt-get -qq install -y libterm-readline-gnu-perl software-properties-common coreutils gnupg2 procps curl apt-transport-https gnupg2 ca-certificates wget gpg-agent

RUN curl -s http://repos.pni-hdri.de/debian_repo.pub.gpg  | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/debian-hdri-repo.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/debian-hdri-repo.gpg
RUN cd /etc/apt/sources.list.d && wget http://repos.pni-hdri.de/resolute-pni-hdri.sources

RUN apt-get -qq update && export DEBIAN_FRONTEND=noninteractive && apt-get -qq dist-upgrade
RUN apt-get -qq update && apt-get -qq install -y net-tools git python3-inotifyx python3-setuptools python3-pytest python3-yaml nxstools python3-requests python3-pytest-cov python3-pninexus

RUN useradd -ms /bin/bash scuser
USER scuser
WORKDIR /home/scuser
