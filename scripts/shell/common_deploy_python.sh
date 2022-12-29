#!/usr/bin/bash

deploy_to=localhost

library=$(basename $(pwd))

[[ ! -z "$1" ]] && deploy_to=$1
ssh -o ConnectTimeout=1 dbadmin@$deploy_to hostname > /dev/null || exit 1
echo Deploying Python library $library to $deploy_to  

[[ -f test_queries.sql ]] && tqf=test_queries.sql

echo Deploying $(ls -la ${library}.py)
scp -o ConnectTimeout=1 ${library}.py create_functions.sql ${tqf} dbadmin@${deploy_to}:/tmp
 [[ $? != 0 ]] && exit 1

tee << '+++' | ssh -o ConnectTimeout=1 dbadmin@$deploy_to library=$library bash
/opt/vertica/bin/vsql -a -U dbadmin -w vertica -v libfile=\'/tmp/${library}.py\' -f /tmp/create_functions.sql
+++
[[ $? != 0 ]] && exit 1
exit 0
