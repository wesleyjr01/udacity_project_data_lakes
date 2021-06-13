# udacity_project_data_lakes
Udacity Dataengineer Degree - Data Lakes with Spark Project


### Bootstrap Actions to install custom software
* https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html
* read the `bootstrap.sh` file. You should add this to `s3` and referente in on your `create_emr.sh` so you can install **pandas** on your cluster.
* Read this article to be able to install packages after creating the cluster: https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/

### Create EMR Cluster
* `$ source create_emr.sh`