#!/usr/bin/bash
# Deploy this lib
# expect one parameter - host name, any node from vertica cluster
# You need to have passwordless ssh to this node as dbadmin. Otherwise, you will need to enter OS dbadmin password 3 times.
#BUILD=Debug
#SKIP_BUILD=y
#DOCKER=y
fencing=
#fencing=not

cd $(dirname $0)
. ../../scripts/shell/common_deploy.sh $@

