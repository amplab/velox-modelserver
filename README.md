#Velox Model Server
=================

[![Build Status](https://amplab.cs.berkeley.edu/jenkins/buildStatus/icon?job=velox testing)](https://amplab.cs.berkeley.edu/jenkins/job/velox%20testing/)

## [Instructions for installing and running Velox](https://github.com/amplab/velox-modelserver/wiki/Installing-and-Running-Velox-on-ec2)


## Git Development Workflow
We are going to try using gitflow as our git development workflow. You can find a description of
the workflow in [this blogpost](http://nvie.com/posts/a-successful-git-branching-model/). Until our
first release, the TL;DR is that we will use the `develop` branch as our "master" branch, merging
PRs into that branch. When you have developed and tested a feature, make sure that the feature is fully
rebased against `develop` and then create a pull request against `develop`. Someone will do a code review and
once all comments are addressed, they will merge the PR into `develop`. Basically, the rule here is never
push directly to master, pull-requests should be able to be auto-merged, and you shouldn't be merging
your own pull requests. The other important component of gitflow is that merged pull-requests shouldn't use
fast-forward `git merge --no-ff myfeature` so that we can easily see the group of commits corresponding
to a pull-request. GitHub does this automatically when merging the pull-request from the web interface.
