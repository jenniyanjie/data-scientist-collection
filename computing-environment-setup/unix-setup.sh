#!/bin/bash -xe

HOME="/home/ubuntu"
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install python-dev python-setuptools libatlas-dev g++
#### Install requirements for Python script
# sudo python2.7 -m pip install referer_parser
### aerospike API
sudo pip install -U aerospike
### dataframe, machine learning of python
sudo pip install -U numpy
sudo apt-get install libatlas-base-dev gfortran
sudo pip install -U scipy
sudo pip install -U pandas
sudo pip install -U statsmodels
sudo pip install -U scikit-learn
### aws
sudo pip install -U boto3
### mySQL
sudo apt-get install libmysqlclient-dev
sudo pip install mysqlclient

### postgresql redshift
sudo apt-get install libpq-dev
sudo pip install psycopg2 
sudo pip install sqlalchemy

### successful algo trading
sudo pip install pandas_datareader

### recommender system
git clone https://github.com/muricoca/crab.git
cd crab
sudo python setup.py install
sudo pip install nose

#### Install xgboost
#1. build the shared library
sudo yum -y install git-core
git clone --recursive https://github.com/dmlc/xgboost $HOME/xgboost
cd $HOME/xgboost; make -j4
#2. build the python package for all user
# cd /home/hadoop/xgboost/python-package; python setup.py develop --user
sudo yum install python-setuptools
cd $HOME/xgboost/python-package; sudo python setup.py install
echo "PYTHONPATH=/home/ubuntu/xgboost/python-package" > $HOME/.bashrc
source $HOME/.bashrc