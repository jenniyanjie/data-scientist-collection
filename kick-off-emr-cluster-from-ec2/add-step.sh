# this script is used to add steps to emr using command-line
# requirement:
# get the cluster-id of cluster
aws emr add-steps \
--cluster-id j-ZXR34TO7PE8O  \
--steps Type=CUSTOM_JAR,Name=intelligent-test,\
ActionOnFailure=CANCEL_AND_WAIT,Jar=command-runner.jar,\
Args=[spark-submit,--deploy-mode,client,--name,intelligent-test,/home/hadoop/the-script-to-run-daily.py,1,2017,05,15,00]
