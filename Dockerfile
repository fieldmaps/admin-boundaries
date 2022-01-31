FROM ubuntu:21.10

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  gdal-bin postgresql-13-postgis-3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN /etc/init.d/postgresql start \
  && su postgres -c 'createdb admin_boundaries' \
  && su postgres -c 'psql -d admin_boundaries -c "CREATE EXTENSION postgis;"'

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY processing ./processing
