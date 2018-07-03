FROM golang:1.9-rc-stretch

RUN groupadd --gid 1000 node \
  && useradd --uid 1000 --gid node --shell /bin/bash --create-home node

RUN wget -O /etc/apt/sources.list https://mirrors.ustc.edu.cn/repogen/conf/debian-http-4-stretch \
&& apt-get update \
&& apt-get install -y unzip xz-utils \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

# gpg keys listed at https://github.com/nodejs/node#release-team
RUN set -ex \
  && for key in \
    94AE36675C464D64BAFA68DD7434390BDBE9B9C5 \
    FD3A5288F042B6850C66B31F09FE44734EB7990E \
    71DCFD284A79C3B38668286BC97EC7A07EDE3FC1 \
    DD8F2338BAE7501E3DD5AC78C273792F7D83545D \
    C4F0DFFF4E8C1A8236409D08E73BC641CC11F4C8 \
    B9AE9905FFD7803F25714661B63B535A4C206CA9 \
    56730D5401028683275BD23C23EFEFE93C4CFFFE \
    77984A986EBC2AA786BC0F66B01FBB92821C587A \
    8FCCA13FEF1D0C2E91008E09770F7A9A5AE15600 \
  ; do \
    gpg --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
    gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
    gpg --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
  done

ENV NODE_VERSION 10.5.0

RUN ARCH= && dpkgArch="$(dpkg --print-architecture)" \
  && case "${dpkgArch##*-}" in \
    amd64) ARCH='x64';; \
    ppc64el) ARCH='ppc64le';; \
    s390x) ARCH='s390x';; \
    arm64) ARCH='arm64';; \
    armhf) ARCH='armv7l';; \
    i386) ARCH='x86';; \
    *) echo "unsupported architecture"; exit 1 ;; \
  esac \
  && curl -fsSLO --compressed "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-$ARCH.tar.xz" \
  && curl -fsSLO --compressed "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt.asc" \
  && gpg --batch --decrypt --output SHASUMS256.txt SHASUMS256.txt.asc \
  && grep " node-v$NODE_VERSION-linux-$ARCH.tar.xz\$" SHASUMS256.txt | sha256sum -c - \
  && tar -xJf "node-v$NODE_VERSION-linux-$ARCH.tar.xz" -C /usr/local --strip-components=1 --no-same-owner \
  && rm "node-v$NODE_VERSION-linux-$ARCH.tar.xz" SHASUMS256.txt.asc SHASUMS256.txt \
  && ln -s /usr/local/bin/node /usr/local/bin/nodejs

# test
# RUN node -v && npm --version

ENV YARN_VERSION 1.7.0

RUN npm config set registry=https://registry.npm.taobao.org && npm install -g yarn@${YARN_VERSION} && yarn config set registry https://registry.npm.taobao.org

RUN go get -u github.com/kardianos/govendor

WORKDIR /go/src/fxdayu.com/dyupdater

ENV ORACLE_BASE=/opt/oracle ORACLE_HOME=/opt/oracle/instantclient_11_2
ENV PKG_CONFIG_PATH=".:${PKG_CONFIG_PATH}" \
PATH="$ORACLE_HOME:$PATH" \
LD_LIBRARY_PATH="$ORACLE_HOME:${LD_LIBRARY_PATH}" \
C_INCLUDE_PATH="$ORACLE_HOME/sdk/include/:${C_INCLUDE_PATH}" \
TNS_ADMIN=$ORACLE_HOME/network/admin NLS_LANG=AMERICAN_AMERICA.UTF8

RUN wget http://download.oracle.com/otn/linux/instantclient/11204/instantclient-basic-linux.x64-11.2.0.4.0.zip

RUN wget http://download.oracle.com/otn/linux/instantclient/11204/instantclient-sdk-linux.x64-11.2.0.4.0.zip

RUN mkdir $ORACLE_BASE \
&& unzip instantclient-basic-linux.x64-11.2.0.4.0.zip -d $ORACLE_BASE \
&& unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip -d $ORACLE_BASE \
&& ln -s $ORACLE_HOME/libclntsh.so.11.1 /usr/lib/libclntsh.so

# test
# RUN echo $ORACLE_HOME $LD_LIBRARY_PATH $C_INCLUDE_PATH

RUN COPY . .

RUN govendor fetch github.com/mattn/go-oci8

RUN make install && make build

RUN chmod +x dyupdater

CMD ["dyupdater"]
