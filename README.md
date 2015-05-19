#Velox Model Server
=================

[![Build Status](https://amplab.cs.berkeley.edu/jenkins/buildStatus/icon?job=velox testing)](https://amplab.cs.berkeley.edu/jenkins/job/velox%20testing/)

##Quickstart

Install and start Velox

```
git clone https://github.com/amplab/velox-modelserver.git
cd velox-modelserver/bin/cluster
pip install fabric
fab install_velox_local:~/
fab start_velox:start_local=y
```

This start a local Velox instance listening on localhost at port 8080.

You can interact with Velox using the REST API:



