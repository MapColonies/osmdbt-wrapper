FROM ubuntu:22.04 as buildOsmdbt

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

FROM ubuntu:22.04 AS buildOsmium

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

FROM node:20 as build

WORKDIR /tmp/buildApp

COPY ./package*.json ./
COPY .husky/ .husky/

RUN npm install
COPY . .
RUN npm run build

FROM node:20.19.0-slim as production

COPY --from=buildOsmdbt /osmdbt /osmdbt
COPY --from=buildOsmium /osmium-tool/build /osmium-tool/build
RUN ln -s /osmium-tool/build/osmium /bin/osmium

# Install required runtime libs for osmdbt and osmium-tool
RUN apt-get update && apt-get install -y \
    dumb-init \
    libpqxx-dev \
    libboost-program-options1.74.0 \
    libyaml-cpp0.7 \
    libexpat1 \
 && rm -rf /var/lib/apt/lists/*

ENV NODE_ENV=production
ENV SERVER_PORT=8080


WORKDIR /usr/src/app

COPY --chown=node:node package*.json ./
COPY .husky/ .husky/

RUN npm ci --only=production

COPY --chown=node:node --from=build /tmp/buildApp/dist .
COPY --chown=node:node ./config ./config

COPY --chown=node:node start.sh .
RUN chmod 755 start.sh


USER node
EXPOSE 8080
ENTRYPOINT ["/usr/src/app/start.sh"]
