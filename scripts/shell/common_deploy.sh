#!/usr/bin/env bash
deploy_to=localhost

root_dir=$(realpath $(dirname ${0})/../..)
target=$(basename $(pwd))
libname=lib${target}
build_flags=" -l -x ${target}"
[[ "${BUILD,,}" == "debug" ]] && build_flags+=" -d" && BUILD=Debug
[[ "${BUILD}" != Debug ]] && BUILD=Release

[[ ! -z "$1" ]] && deploy_to=$1
ssh -o ConnectTimeout=1 dbadmin@$deploy_to hostname > /dev/null 
[[ $? != 0 ]] && echo Cannot ssh to dbadmin@$deploy_to && exit 1
echo Deploying SKIP_BUILD=$SKIP_BUILD BUILD=$BUILD build_flags ${build_flags} to $deploy_to fencing $fencing

if [[ -z "${SKIP_BUILD}" ]]; then
  ${root_dir}/build.sh ${build_flags}
  [[ $? != 0 ]] && echo Error in build && exit 1
fi

[[ -f test_queries.sql ]] && tqf=test_queries.sql


echo Stripping "$(ls -la ${root_dir}/BUILD/${BUILD}/${libname}.so)"
strip ${root_dir}/BUILD/${BUILD}/${libname}.so
echo Deploying "$(ls -la ${root_dir}/BUILD/${BUILD}/${libname}.so)"
scp -o ConnectTimeout=1 ${root_dir}/BUILD/${BUILD}/${libname}.so create_functions.sql ${tqf} dbadmin@${deploy_to}:/tmp
[[ $? != 0 ]] && exit 1

tee << '+++' | ssh -o ConnectTimeout=1 dbadmin@$deploy_to libname=$libname fencing=$fencing bash
/opt/vertica/bin/vsql -a -U dbadmin -v libfile=\'/tmp/${libname}.so\' -v fencing=$fencing -f /tmp/create_functions.sql
+++
[[ $? != 0 ]] && exit 1
