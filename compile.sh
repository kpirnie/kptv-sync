#!/usr/bin/env bash

# get the version number
TMPVER=`cat /etc/os-release | grep VERSION_ID`
VER=$(echo "$TMPVER" | sed "s/VERSION_ID=//")
VER=${VER:1:-1}

# make sure pip is indeed installed
apt-get install -y python3-pip

# make sure zip is installed
apt-get install zip

# now we need to make sure that python3-dev
apt-get install -y python3-dev

# let's make sure pip is up to date
python3 -m pip install --upgrade pip regex pyinstaller --break-system-packages --root-user-action ignore
python3 -m pip install --upgrade m3u-parser pymysql --break-system-packages --root-user-action ignore
python3 -m pip install --upgrade mysql-connector-python regex --break-system-packages --root-user-action ignore

# get the path to this script
CODEPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )";

# make a directory for the releases
mkdir -p $CODEPATH/release;

# try to compile
pyinstaller \
    --distpath $CODEPATH/release/ \
    --clean \
    --hidden-import pymysql \
    -F \
    -n kptv \
    -p $CODEPATH/src/ \
    --collect-all pymysql \
    --copy-metadata pymysql \
    --hidden-import pymysql \
    --hidden-import pymysql.cursors \
    --hidden-import pymysql.connections \
    --hidden-import pymysql.converters \
    --hidden-import pymysql.err \
    --hidden-import pymysql.protocol \
    --hidden-import pymysql.charset \
    --hidden-import pymysql.constants.CLIENT \
    --hidden-import pymysql.constants.COMMAND \
    --hidden-import pymysql.constants.ER \
    --hidden-import pymysql.constants.FIELD_TYPE \
    --hidden-import pymysql.constants.FLAG \
    --hidden-import pymysql.constants.SERVER_STATUS \
    --hidden-import pymysql._auth \
    --hidden-import pymysql.optionfile \
    --hidden-import pymysql.times \
    --hidden-import pymysql.util \
    --hidden-import ssl \
    --hidden-import socket \
    --hidden-import threading \
    --hidden-import queue \
$CODEPATH/src/main.py

# find and remove the PYC files
find . -type f -name "*.pyc" -exec rm -f {} \;
find . -type d -name "__pycache*" -exec rm -rf {} \;

# remove the build directory
rm -rf $CODEPATH/build

# set the executable bit
chmod +x $CODEPATH/release/kptv

# change the ownership back to me
chown -R kpirnie:kpirnie $CODEPATH/release/kptv

# copy it to our local bin
cp $CODEPATH/release/kptv /usr/local/bin/

# copy our config
cp $CODEPATH/src/.kptvconf /
