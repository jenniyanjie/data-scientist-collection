#!/bin/bash -xe

# Parse arguments
s3_bucket="$1"
s3_bucket_script="$s3_bucket/script.tar.gz"

# Download compressed script tar file from S3
aws s3 cp $s3_bucket_script /home/hadoop/script.tar.gz

# Untar file
tar zxvf "/home/hadoop/script.tar.gz" -C /home/hadoop/

#### Install requirements for Python script
# sudo python2.7 -m pip install referer_parser
sudo pip install -U aerospike
sudo pip install -U numpy
sudo pip install -U pandas
sudo pip install -U boto3
sudo pip install -U scikit-learn

#### Install xgboost not necessary since changing to sparkML
#1. build the shared library
#sudo yum -y install git-core
#git clone --recursive https://github.com/dmlc/xgboost /home/hadoop/xgboost
#2. build the python package for all user
#cd /home/hadoop/xgboost/python-package; python setup.py develop --user
#sudo yum install python-setuptools
#cd /home/hadoop/xgboost/python-package; sudo python setup.py install
#echo "PYTHONPATH=/home/hadoop/xgboost/python-package" > /home/hadoop/.bashrc
#source /home/hadoop/.bashrc
