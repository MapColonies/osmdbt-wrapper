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

RUN git clone git://github.com/openstreetmap/osmdbt.git && \
    cd osmdbt && \
    mkdir build && cd build && cmake -DBUILD_PLUGIN=OFF .. && cmake --build . && make && make install

FROM ubuntu:20.04 as production

ENV DEBIAN_FRONTEND=noninteractive
ENV workdir /app
ARG NODE_VERSION=14.x

COPY --from=build /osmdbt /osmdbt

WORKDIR ${workdir}

RUN chmod g+w /app

COPY package*.json /app/

RUN apt-get update -yq \
    && apt-get -yq install curl libpqxx-dev libboost-program-options-dev libyaml-cpp-dev libboost-filesystem-dev \
    && curl -L https://deb.nodesource.com/setup_${NODE_VERSION} | bash \
    && apt-get install -yq nodejs

RUN npm i --only=production

COPY start.sh .
COPY index.mjs .
COPY ./config ./config

RUN chgrp root ${workdir}/start.sh && chmod -R a+rwx ${workdir} && \
    mkdir /.postgresql && chmod g+w /.postgresql

RUN useradd -ms /bin/bash user && usermod -a -G root user
USER user

CMD ./start.sh
