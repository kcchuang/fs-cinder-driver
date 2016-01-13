#!/bin/bash -xe

export CINDER_DIR=./cinder
export CINDER_REPO_URL=https://git.openstack.org/openstack/cinder
export CINDER_TEST_DIR=cinder/tests/unit
export FORTUNET_DRIVER_DIR=cinder/volume/drivers

if [ -d "$CINDER_DIR" ]; then
    rm -rf $CINDER_DIR
fi

git clone $CINDER_REPO_URL --depth=1

if [ ! -d "$CINDER_DIR/$FORTUNET_DRIVER_DIR" ]; then
    mkdir $CINDER_DIR/$FORTUNET_DRIVER_DIR
fi

cp ./fs3000.py $CINDER_DIR/$FORTUNET_DRIVER_DIR/ -r
cp ./test_fs3000.py $CINDER_DIR/$CINDER_TEST_DIR/ -r

cd $CINDER_DIR

./run_tests.sh -V test_fs3000
#./run_tests.sh -p

cd ..
