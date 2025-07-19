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
python3 -m pip install --upgrade pip regex pyinstaller --break-system-packages
python3 -m pip install --upgrade m3u-parser --break-system-packages

# get the path to this script
CODEPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )";

# make a directory for the releases
mkdir -p $CODEPATH/release;

# try to compile
pyinstaller \
    --distpath $CODEPATH/release/ \
    --clean \
    --hidden-import mysql.connector \
    --hidden-import mysql.connector.plugins.mysql_native_password \
    -F \
    -n kptv \
    -p $CODEPATH/src/ \
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
