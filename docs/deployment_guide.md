##Velox Deployment Guide


###Running Velox Locally
1. `git clone https://github.com/amplab/velox-modelserver.git; cd velox-modelserver`
1. `cd bin/cluster`
1. edit `velox_config.py` to configure your Velox deployment (see [Configuration](#secconfig) for details).
1. Install velox and dependencies `fab install_velox_local:~/`.
This downloads and starts `Etcd`, compiles Velox, and sets some environment
variables to the correct values for running locally. The command takes the location where
etcd should be downloaded as an argument
(or the path to the existing directory if you already have etcd downloaded).
1. Start velox: `fab start_velox:start_local=y`

Now try it out. Make a prediction
`curl -H "Content-Type: application/json" -d '{"context": 4, "uid":1000}' http://localhost:8080/predict/matrixfact`.
And add an observation
`curl -H "Content-Type: application/json" -d '{"context": 4, "uid":4, "score":1.3}' http://localhost:8080/observe/matrixfact`

And retrain in Spark:
`curl http://localhost:8080/observe/matrixfact`


### Running Velox On a Cluster

####Set environment variables:
When running Velox on an AWS cluster, there are three environment variables that must be
set. These allow the script to access your AWS account to launch machines and then to SSH into
the cluster. You must export your AWS credentials as environment variables in order
for boto to launch instances, and you must set `VELOX_CLUSTER_KEY` to point to your
AWS key pair so the script can ssh into the machines to set up and run the cluster.

```
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export VELOX_CLUSTER_KEY=~/.ssh/my_key_pair.pem

```

####Launch a cluster
+ `fab launch_ec2_cluster:cluster_name=my-application,cluster_size=3,spot_price=1.5,localkey=my_github_ssh_key,keyname=aws_knox` to launch a cluster
+ edit `velox_config.py` to configure your Velox deployment (see [Configuration](#secconfig) for details).
+ `fab start_velox` to start the cluster

__TODO__ remove localkey once Velox is open-sourced

#### Stopping Velox
To stop Velox on all nodes, run `fab stop_velox`.

###<a name="secconfig"></a>Configuration
Velox is configured by defining the `config` variable located in
[`bin/cluster/velox_config.py`](../bin/cluster/velox_config.py).
`config` is a Python dictionary with three top-level
keys: `sparkMaster`, `sparkDataLocation`, and `models`.

+ `sparkMaster` tells Velox how to connect to a Spark cluster. Internally, it is used
when creating a [SparkConf](https://spark.apache.org/docs/1.3.1/api/scala/#org.apache.spark.SparkConf)
object as the argument to `SparkConf.setMaster()`. It can either be of the form "spark://masterurl:7077"
to connect to an existing standalone cluster, or "local" (or "local[4]") to connect
to a local Spark instance. If using a local Spark instance, Velox will automatically
start one at runtime and shut it down when Velox is stopped. Spark can be used locally or
as a standalone cluster whether you are running Velox locally or on a cluster. If using a
local Spark instance when running a Velox cluster, each Velox node will start its own
local Spark instance.
+ `sparkDataLocation` is a directory location that Velox uses to persist data for sharing
across multiple SparkContexts. It can be set to any path that Spark can read from and write to.
Common choices include the local filesystem, HDFS, and Tachyon.
+ `models` is a a list of model configuration objects. See the next section for model-specific configuration.

####Model Configuration
To deploy a model in Velox, define a model configuration object. A model configuration
is a Python dictionary with a single key-value pair. The key is the name you want to give this model instance,
and the value defines the parameters of the model as another dict. The relevant
model parameters are

+ `onlineUpdateDelayMillis` - how frequently online model updates should be performed
+ `batchRetrainDelayInMillis` - how frequently batch model training in Spark should be performed
+ `dimensions` - the number of features in your user model. Velox expects that the size of the feature
vector returned in [`Model.computeFeatures`](../veloxms-core/src/main/scala/edu/berkeley/veloxms/models/Model.scala) matches this dimensions setting.
+ `modelType` - the type of the model to deploy. This must be the name of a class that
extends the `Model` abstract class. See [Adding Your Own Model](#sec-adding-a-model)

For example, this model configuration defines a single model named
'hello-world' of type `MatrixFactorization` and uses a local Spark instance.

```python
import json
helloworld_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 500000,
        'dimensions': 50,
        'modelType': 'MatrixFactorizationModel',
        }

config = {
        'sparkMaster': "local[2]",
        'sparkDataLocation': "~/velox-data",
        'models': [
            { 'hello-world': json.dumps(helloworld_config) },
            ]
        }
```


An example model configuration is provided in
[`velox_config.py`](../bin/cluster/velox_config.py) as `matrixfact_config`. 


###Dependencies
+ Java 7
+ Scala 2.10
+ Maven 3
+ Spark 1.3.1
+ [Etcd](https://github.com/coreos/etcd/releases/tag/v2.0.10)
+ Python
    + [`boto`](http://docs.pythonboto.org/en/latest/)
    + [`fabric`](http://www.fabfile.org/installing.html) >= 1.10.0 and dependencies (Paramiko >= 1.10.0)




##<a name="sec-adding-a-model"></a>Adding Your Own Model
__TODO__ How to add a model

