#!/bin/bash 
set -x 
timestr=`date +%Y%m%d%H%M` 
current_path=`pwd`
case "`uname`" in
    Linux)
                bin_abs_path=$(readlink -f $(dirname $0))
                ;;
        *)
                bin_abs_path=`cd $(dirname $0); pwd`
                ;;
esac
base=${bin_abs_path}/../
export LANG=en_US.UTF-8
export BASE=$base/../

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

ps -ef| grep canal.client.CanalForwordMain |grep ${base} | grep -v grep | awk '{print $2}'| xargs kill -9

$JAVA -cp $base/lib/*: -DCANAL_CLIENT_CONF=$base canal.client.CanalForwordMain 2>&1 >canal_client_$timestr.log &