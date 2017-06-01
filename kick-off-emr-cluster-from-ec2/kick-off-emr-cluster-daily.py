#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
1. create the cluster with m4.xlarge + 16 X r3.xlarge
Created on April 6, 2016
@author: thom.hopmans
@author: Jennifer
"""

import logging
import os, sys
from datetime import datetime, timedelta
import time
import tarfile
import boto3
import botocore

def setup_logging(default_level=logging.WARNING):
    """
    Setup logging configuration
    """
    logging.basicConfig(format='%(asctime)s   %(message)s', level=default_level)
    return logging.getLogger('ProcessingDMPusingSPARK')


def terminate(error_message=None):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


class DeployPySparkScriptOnAws(object):
    """
    Programmatically deploy a local PySpark script on an AWS cluster
    """

    def __init__(self, homepath, yyyy, mm, dd):
        '''
        note about the credentials setting of ec2:
        1. setup up the aws config on the ec2
        $aws config
        AWS Access Key ID [****************XXXX]:
        AWS Secret Access Key [****************XXXX]:
        Default region name [ap-southeast-1]:
        Default output format [None]:

        2. double check the config and credential files in ~/.aws
        there should be an user profile with the name self.user ??? double check
        $vi config
        [profile jennifer]
        region = ap-southeast-1
        [default]
        region = ap-southeast-1

        $vi credential
        [jennifer]
        aws_access_key_id = <key_id>
        aws_secret_access_key = <access_key>
        [default]
        aws_access_key_id = <key_id>
        aws_secret_access_key = <access_key>


        '''
        self.homepath = homepath
        self.app_name = "application_spark_deploy"          # Application name
        self.ec2_key_name = "jennifer_emr_key"              # Key name to use for cluster
        self.job_flow_id = None                             # Returned by AWS in start_spark_cluster()
        self.job_name = None                                # Filled by generate_job_name()
        self.path_script = os.path.join(homepath, "spark_script/")  # Path of Spark script to be deployed on AWS Cluster
        self.s3_bucket_logs = "aws-logs-XXXXXXXXXXXX-ap-southeast-1"   # S3 Bucket to store AWS EMR logs
        self.s3_bucket = "s3bucket-emr-spark"         # S3 Bucket to store temporary files
        self.s3_region = 's3-ap-southeast-1.amazonaws.com'  # S3 region to specifiy s3Endpoint in s3-dist-cp step
        self.user = 'jennifer'                              # Define user name, yes, I am jennifer!
        self.files_to_keep = 5                              # to keep the uploaded files in S3 for n days, because this script
                                                            # is running on everyday, then each day, lots of files would be save to s3.
                                                            # I make a strategy of saving those files for 5 days.

        #-------------to process the bid request of yyyy/mm/dd---------------------------------------------------#
        self.yyyy = yyyy
        self.mm = mm
        self.dd = dd

    def run(self):
        session = boto3.Session(profile_name=self.user)    # Select AWS IAM profile
        s3 = session.resource('s3')                         # Open S3 connection
        self.generate_job_name()                            # Generate job name
        self.temp_bucket_exists(s3)                         # Check if S3 bucket to store temporary files in exists
        self.tar_python_script()                            # Tar the Python Spark script
        self.upload_temp_files(s3)                          # Move the Spark files to a S3 bucket for temporary files
        c = session.client('emr')                           # Open EMR connection
        self.start_spark_cluster(c)                         # Start Spark EMR cluster

        self.step_spark_submit(c)                             # Add step 'spark-submit'
        # self.describe_status_until_terminated(c)            # Describe cluster status until terminated
        self.remove_old_folders(s3)                          # Remove files from the temporary files S3 bucket

    def generate_job_name(self):
        self.job_name = "{}.{}.{}".format(self.app_name,
                                          self.user,
                                          datetime.now().strftime("%Y%m%d.%H%M%S.%f"))

        # the folder to download the model. the model is created on yesterday's data, the folder name is yesterday
        yesterday = datetime(int(self.yyyy), int(self.mm), int(self.dd)) - timedelta(days = 1)
        self.model_param_folder = "intelligent-model/{}".format(yesterday.strftime('%Y-%m-%d'))

        # to define the prefix of the files to delete:

        # 1. main process folder
        self.prefix_folder_to_del = "{}.{}.{}".format(self.app_name,
                                          self.user,
                                          (datetime.now() - timedelta(days = self.files_to_keep)).strftime("%Y%m%d"))
        # 2. csv data folder
        self.prefix_csv_folder_to_del = "csv_output/{}".\
                                    format((datetime.now() - timedelta(days = self.files_to_keep)).strftime("%Y-%m-%d"))

        # 3. model folder
        self.prefix_model_folder_to_del = "intelligent_model/{}".\
                                    format((datetime.now() - timedelta(days = self.files_to_keep)).strftime("%Y-%m-%d"))



    def temp_bucket_exists(self, s3):
        """
        Check if the bucket we are going to use for temporary files exists.
        :param s3:
        :return:
        """
        try:
            s3.meta.client.head_bucket(Bucket=self.s3_bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                terminate("Bucket for temporary files does not exist")
            terminate("Error while connecting to Bucket")
        logger.info("S3 bucket for temporary files exists")

    def tar_python_script(self):
        """
        :return:
        """
        # Create tar.gz file
        t_file = tarfile.open(os.path.join(self.homepath, "files/script.tar.gz"), 'w:gz')
        # Add Spark script path to tar.gz file
        files = os.listdir(self.path_script)
        for f in files:
            t_file.add(self.path_script + f, arcname=f)
        # List all files in tar.gz
        for f in t_file.getnames():
            logger.info("Added %s to tar-file" % f)
        t_file.close()

    def upload_temp_files(self, s3):
        """
        Move the PySpark script files to the S3 bucket we use to store temporary files
        :param s3:
        :return:
        """
        # Shell file: setup (download S3 files to local machine)
        s3.Object(self.s3_bucket, self.job_name + '/setup.sh')\
          .put(Body=open(os.path.join(self.homepath,'files/setup.sh'), 'rb'), ContentType='text/x-sh')

        # Shell file: download the model related files
        s3.Object(self.s3_bucket, self.job_name + '/download_model_param.sh')\
          .put(Body=open(os.path.join(self.homepath, 'files/download_model_param.sh'), 'rb'), ContentType='text/x-sh')

        # Shell file: terminate idle cluster
        s3.Object(self.s3_bucket, self.job_name + '/terminate_idle_cluster.sh')\
          .put(Body=open(os.path.join(self.homepath,'files/terminate_idle_cluster.sh'), 'rb'), ContentType='text/x-sh')

        # Compressed Python script files (tar.gz)
        s3.Object(self.s3_bucket, self.job_name + '/script.tar.gz')\
          .put(Body=open(os.path.join(self.homepath,'files/script.tar.gz'), 'rb'), ContentType='application/x-tar')
        logger.info("Uploaded files to key '{}' in bucket '{}'".format(self.job_name, self.s3_bucket))
        return True

    def remove_old_folders(self, s3):
        """
        to avoid too much files in s3 (to increase the charge)
        remove files from bucket by identifying the prefix of the files
        :param s3:
        :return:
        """
        bucket = s3.Bucket(self.s3_bucket)
        for key in bucket.objects.all():
            if (key.key.startswith(self.prefix_folder_to_del) or
                key.key.startswith(self.prefix_csv_folder_to_del) or
                key.key.startswith(self.prefix_model_folder_to_del)):
                key.delete()
                logger.info("Removed '{}' from bucket".format(key.key))

    def start_spark_cluster(self, c):
        """
        :param c: EMR client
        :return:
        """
        response = c.run_job_flow(
            Name=self.job_name, # "DMP_test"
            LogUri="s3://{}/elasticmapreduce/".format(self.s3_bucket_logs),
            ReleaseLabel="emr-5.4.0",
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'EmrMaster',
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'BidPrice': '0.10', # m4.large: 0.10
                        'InstanceType': 'm4.large',
                        'InstanceCount': 1,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': 32     },
                                    'VolumesPerInstance': 1
                                }
                                                    ]
                                            }
                    },
                    {
                        'Name': 'EmrCore',
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'BidPrice': '0.15', # r3.2xlarge: 0.36, r3.xlarge: 0.15
                        'InstanceType': 'r3.xlarge',  # node instance type
                        'InstanceCount': 16, # number of working node
                    },
                ],
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': False, # False: shut down the cluster after job finishes
                'TerminationProtected': False,
                # 'HadoopVersion': 'string',
                'Ec2SubnetId': 'subnet-XXXXXXXX',
                'EmrManagedMasterSecurityGroup': 'sg-XXXXXXXX',
                'EmrManagedSlaveSecurityGroup': 'sg-XXXXXXXX',
            },
            Applications=[{'Name': 'Spark'}, {'Name': 'Hadoop'}], #, {'Name': 'Zeppelin'} , {'Name': 'Hive'}
            # Configurations=[
                # {
                    # 'Classification': 'spark',
                    # 'Properties': {
                        # "maximizeResourceAllocation":"true" # this config is not good to use than default: DynamicResourceAllocation
                    # }
                # },
            # ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            BootstrapActions=[
                {
                    'Name': 'setup',
                    'ScriptBootstrapAction': {
                        'Path': 's3n://{}/{}/setup.sh'.format(self.s3_bucket, self.job_name),
                        'Args': [
                            's3://{}/{}'.format(self.s3_bucket, self.job_name)
                        ]
                    }
                }, # copy the script to master and install python library
#==============================================================================
#                 ##### command this segment if the model is not trained yesterday
#                 {
#                     'Name': 'download model param',
#                     'ScriptBootstrapAction': {
#                         'Path': 's3n://{}/{}/download_model_param.sh'.format(self.s3_bucket, self.job_name),
#                         'Args': [
#                             's3://{}/{}'.format(self.s3_bucket, self.model_param_folder)
#                             ]
#                     }
#                 }, # download the model-parameter
#==============================================================================
                ##### command this segment if the model is not trained yesterday
                {
                    'Name': 'idle timeout',
                    'ScriptBootstrapAction': {
                        'Path':'s3n://{}/{}/terminate_idle_cluster.sh'.format(self.s3_bucket, self.job_name),
                        'Args': ['3600', '300']
                    }
                },
            ],
            ScaleDownBehavior='TERMINATE_AT_INSTANCE_HOUR'
        )
        # Process response to determine if Spark cluster was started, and if so, the JobFlowId of the cluster
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            terminate("Could not create EMR cluster (status code {})".format(response_code))

        logger.info("Created Spark EMR-5.3.1 cluster with JobFlowId {}".format(self.job_flow_id))

    def describe_status_until_terminated(self, c):
        """
        :param c:
        :return:
        """
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.job_flow_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
            logger.info(state)
            time.sleep(30)  # Prevent ThrottlingException by limiting number of requests

    def step_spark_submit(self, c):
        """
        :param c:
        :param stepName = "2017031600"
        :param arguments = "2 201703 16 00"
        :return:
        """

        spark_steps = []
#==============================================================================
#         # extracting_training_data steps
#         for i in xrange(0, 10, 5): # for now try to grab first 5 hours' data only
#             hh = '{:02d}'.format(i)
#             stepName = 'extract_training_data_{0}{1}{2}{3}'.format(self.yyyy, self.mm, self.dd, hh)
#             single_step = {
#                             'Name': stepName,
#                             'ActionOnFailure': 'TERMINATE_CLUSTER', # action: TERMINATE_CLUSTER | TERMINATE_JOB_FLOW | CANCEL_AND_WAIT | CONTINUE
#                             'HadoopJarStep': {
#                                 'Jar': 'command-runner.jar',
#                                 'Args': [
#                                     "spark-submit",
#                                     "--deploy-mode", "client",
#                                     "--name", stepName,
#                                     "/home/hadoop/pi.py",
#                                     "5", self.yyyy, self.mm, self.dd, hh
#                                         ]
#                                                 }
#                         }
#             spark_steps.append(single_step)
#==============================================================================

        # writing_to_dmp steps
        for i in xrange(0, 24, 2): # incluse 00 but not 24
            hh = '{:02d}'.format(i)
            stepName = 'write_to_dmp_{0}{1}{2}{3}'.format(self.yyyy, self.mm, self.dd, hh)
            single_step = {
                            'Name': stepName,
                            'ActionOnFailure': 'TERMINATE_CLUSTER', # action: TERMINATE_CLUSTER | TERMINATE_JOB_FLOW | CANCEL_AND_WAIT | CONTINUE
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    "spark-submit",
                                    "--deploy-mode", "client",
                                    "--name", stepName,
                                    "/home/hadoop/spark_job.py",
                                    "2", self.yyyy, self.mm, self.dd, hh
                                        ]
                                                }
                            }
            spark_steps.append(single_step)

        response = c.add_job_flow_steps(
            JobFlowId = self.job_flow_id,
            Steps = spark_steps
        )
        logger.info("Added totally {} steps with 'spark-submit'".format(len(spark_steps)))
        time.sleep(1)  # Prevent ThrottlingException
    def step_copy_data_between_s3_and_hdfs(self, c, src, dest):
        """
        Copy data between S3 and HDFS (not used for now)
        :param c:
        :return:
        """
        response = c.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[{
                    'Name': 'Copy data from S3 to HDFS',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "s3-dist-cp",
                            "--s3Endpoint=s3-eu-west-1.amazonaws.com",
                            "--src={}".format(src),
                            "--dest={}".format(dest)
                        ]
                    }
                }]
        )
        logger.info("Added step 'Copy data from {} to {}'".format(src, dest))


logger = setup_logging()

if __name__ == "__main__":
    try:
        yyyy = sys.argv[1] if len(sys.argv[1]) == 4 else '20' + sys.argv[1]
        mm = sys.argv[2] if len(sys.argv[2]) == 2 else '0' + sys.argv[2]
        dd = sys.argv[3] if len(sys.argv[3]) == 2 else '0' + sys.argv[3]

    except IndexError:
        print('usage: python kick-off-emr-cluster-daily.py yyyy mm dd')
        print('to process the spark on emr cluster on yyyy/mm/dd')
        sys.exit(1)

    projPath = '/home/ubuntu/project/data-scientist-collection/kick-off-emr-cluster-from-ec2' # the loaction of this file
    DeployPySparkScriptOnAws(projPath, yyyy, mm, dd).run()