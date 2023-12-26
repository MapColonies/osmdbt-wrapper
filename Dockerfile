ARG NODE_VERSION=16

FROM ubuntu:20.04 as buildOsmdbt

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y git \
    cmake \
    g++ \
    libosmium2-dev \
    libboost-filesystem-dev \
    libboost-program-options-dev \
    libyaml-cpp-dev \
    libpqxx-dev

RUN git clone https://github.com/openstreetmap/osmdbt.git && \
    cd osmdbt && \
    mkdir build && cd build && cmake -DBUILD_PLUGIN=OFF .. && cmake --build . && make && make install

FROM ubuntu:20.04 AS buildOsmium

ENV DEBIAN_FRONTEND=noninteractive
ARG OSMIUM_TOOL_TAG=v1.16.0
ARG PROTOZERO_TAG=v1.7.1
ARG LIBOSMIUM_TAG=v2.20.0

RUN apt-get -y update && apt -y install \
  make \
  cmake \
  g++ \
  libboost-dev \
  libboost-system-dev \
  libboost-filesystem-dev \
  libboost-program-options-dev \
  libexpat1-dev \
  libbz2-dev \
  libpq-dev \
  libopencv-dev \
  zlib1g-dev \
  git-core

RUN git clone -b ${OSMIUM_TOOL_TAG} --single-branch https://github.com/osmcode/osmium-tool ./osmium-tool && \
  git clone -b ${PROTOZERO_TAG} --single-branch https://github.com/mapbox/protozero ./protozero && \
  git clone -b ${LIBOSMIUM_TAG} --single-branch https://github.com/osmcode/libosmium ./libosmium && \
  cd osmium-tool && \
  mkdir build && \
  cd build && \
  cmake .. && \
  make

FROM node:${NODE_VERSION} as buildApp

WORKDIR /tmp/buildApp

COPY ./package*.json ./

RUN npm install
COPY . .
RUN npm run build

FROM ubuntu:20.04 as production

ENV DEBIAN_FRONTEND=noninteractive
ENV workdir /app
ARG NODE_VERSION

COPY --from=buildOsmdbt /osmdbt /osmdbt
COPY --from=buildOsmium /osmium-tool/build /osmium-tool/build
RUN ln -s /osmium-tool/build/osmium /bin/osmium

WORKDIR ${workdir}

RUN apt-get update \
    && apt-get -yq install curl \
    && curl -L https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash \
    && apt-get -yq install nodejs libpqxx-dev libboost-program-options-dev libyaml-cpp-dev libboost-filesystem-dev

COPY ./package*.json ./

RUN npm ci --only=production

COPY --from=buildApp /tmp/buildApp/dist .
COPY ./config ./config
COPY start.sh .

RUN chgrp root ${workdir}/start.sh && chmod -R a+rwx ${workdir} && \
    mkdir /.postgresql && chmod g+w /.postgresql

# uncomment while developing to make sure the docker runs on openshift
# RUN useradd -ms /bin/bash user && usermod -a -G root user
# USER user

ENTRYPOINT [ "/app/start.sh" ]
