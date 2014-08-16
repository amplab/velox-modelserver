#!/usr/bin/env sh

TACHYON_HOME=~/tachyon

if [ ! -d "$TACHYON_HOME" ]; then
  git clone https://github.com/dcrankshaw/tachyon.git $TACHYON_HOME
fi
cd $TACHYON_HOME
# check out a known working version of tachyon
# this SHA is head of branch nrr2 as of 8/12/14
# git show-ref --verify --quiet refs/heads/<branch-name>
# $? == 0 means local branch with <branch-name> exists. 
git checkout velox-build
mvn package -DskipTests
mvn install:install-file -Dfile=core/target/tachyon-0.6.0-SNAPSHOT-jar-with-dependencies.jar -DgroupId=org.tachyonproject -DartifactId=tachyon-parent -Dversion=0.6.0-SNAPSHOT -Dpackaging=jar
cd -
