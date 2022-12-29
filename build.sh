#!/bin/bash
# This is build script for UDxUtilities repo

set -e

cd $(dirname $0)

BUILD=Release
CPU=`nproc`
VERBOSE=
VERBOSE_OPT=
VERTICA=

while getopts "adlqVx:" opt; do
    case $opt in
        a) ASAN=1;;
        d) BUILD=Debug;;
        q) QUICK=1;;
        V) VERBOSE=1;;
        x) TARGET=$OPTARG;;
    esac
done

if [ -n "$VERBOSE" ]; then
  VERBOSE_OPT="VERBOSE=1"
fi

DEFINES="-DCMAKE_BUILD_TYPE:STRING=$BUILD"
if [ -n "$ASAN" ]; then
    DEFINES="$DEFINES -DUSE_ASAN=1"
fi

root_dir=$(pwd)
build_dir=BUILD/"${BUILD}"
GEN_CMD="cmake $root_dir -Wdev $DEFINES"
BUILD_CMD="make -j$CPU $VERBOSE_OPT -r $TARGET"

if [ -z "$QUICK" ]; then
    echo "Cleaning workspace"
    rm -rf $build_dir
    mkdir -p $build_dir
    cd $build_dir
    echo "Generating project in $build_dir ($GEN_CMD)"
    $GEN_CMD
else
    if [ ! -d "$build_dir" ]; then
        echo "Cannot build quick because build workspace ($build_dir) does not exist"
        exit 1
    fi
    cd $build_dir
fi

echo "Building ($BUILD_CMD)"
$BUILD_CMD
