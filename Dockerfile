ARG NODE_VERSION=16

FROM ubuntu:20.04 as build

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

COPY --from=build /osmdbt /osmdbt

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
