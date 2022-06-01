FROM debian:stretch

MAINTAINER DESY, Jan Kotanski <jankotan@gmail.com>
RUN /bin/bash -c 'echo "deb http://ftp.de.debian.org/debian stretch main" > /etc/apt/sources.list.d/debian9de.list'
RUN apt-get -qq update && apt-get -qq install -y libterm-readline-gnu-perl software-properties-common coreutils gnupg2 procps apt-utils curl apt-transport-https gnupg2 ca-certificates
RUN curl http://repos.pni-hdri.de/debian_repo.pub.gpg | apt-key add -
RUN add-apt-repository "deb http://repos.pni-hdri.de/apt/debian stretch main" -y
RUN apt-get -qq update && apt-get -qq -y dist-upgrade
RUN apt-get -qq update && apt-get  install -y  apt-utils net-tools git python3-inotifyx python3-setuptools python3-pytest python3-yaml

RUN useradd -ms /bin/bash scuser
USER scuser
WORKDIR /home/scuser