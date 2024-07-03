#!/bin/bash
# OssImport Console for Linux

#######################################################
# Define function
#######################################################

function usage() {
  echo "deploy"
  echo "    Deploy the module of import. [all | client],all is must for stand alone or distributed，client just for distributed"
  echo "obsync"
  echo "    Submit task form the task config file."
  echo "start"
  echo "    Start import service."
  echo "stop"
  echo "    Stop import service."
  echo "stat"
  echo "    View the status of all tasks."
  echo "version"
  echo "    Print the version of ossImport."
  echo "kill"
  echo "    kill all process of ossImport."
}

function check_command_args() {
  num_of_args="$#"

  # check the number of args
  if [ ${num_of_args} -gt 2 -o ${num_of_args} -eq 0 ]; then
    usage
    exit 1
  fi

  # check command
  command=$1
  case ${command} in
  "deploy" | "Deploy")
    if [ ${num_of_args} -ne 2 ]; then
      usage
      exit
    fi
    ;;
  "start" | "Start")
    if [ ${num_of_args} -ne 1 ]; then
      usage
      exit 1
    fi
    ;;
  "stop" | "Stop")
    if [ ${num_of_args} -ne 1 ]; then
      usage
      exit 1
    fi
    ;;
  "submit" | "Submit") ;;

  "clean" | "Clean")
    if [ ${num_of_args} -ne 2 ]; then
      usage
      exit
    fi
    ;;
  "stat" | "Stat")
    if [ ${num_of_args} -ne 1 ]; then
      usage
      exit 1
    fi
    ;;
  "retry" | "Retry") ;;

  "version" | "Version") ;;

  "kill" | "Kill") ;;
  *)
    usage
    exit 1
    ;;
  esac
}

function deploy_import_service() {
  case ${module} in
  "all")
    ./bin/server >/dev/null 2>&1 &
    sleep 1s
    ./bin/client >/dev/null 2>&1 &
    sleep 1s
    echo "Info: deploy import server and client completed."
    ;;
  "server")
    ./bin/server >/dev/null 2>&1 &
    sleep 1s
    echo "Info: deploy import server completed."
    ;;
  "client")
    ./bin/client >/dev/null 2>&1 &
    sleep 1s
    echo "Info: deploy import client completed."
    ;;
  *)
    usage
    ;;
  esac
  exit 1
}

function start_import_service() {
  ./bin/import start
  echo "Info: start import service completed."
}

function stop_import_service() {
  ./bin/import stop
  echo "Info: stop import service completed."
}

function submit_job() {
  ./bin/import obsync
}

function kill_server() {
  ps -ef | grep ./bin/client | grep -v grep | awk '{print $2}' | xargs kill -9
  ps -ef | grep ./bin/server | grep -v grep | awk '{print $2}' | xargs kill -9
}

function clean_job() {
  if [ -z "$task_name" ]; then
    echo "Error: task_name:${task_name} invalid."
    exit 3
  fi
  ./bin/import clean "${task_name}"
}

function stat_job() {
  out="$(./bin/import stat)"
  echo "${out}"
}

function retry_failed_tasks() {
  # retry
  if [ -z "$task_name" ]; then
    echo "Error: task_name:${task_name} invalid."
    exit 3
  fi
  ./bin/import retry "${task_name}"
}

function version_import() {
  ./bin/import version
}

#######################################################
# Start of main
#######################################################

command=
task_name=
args="$*"
module=

# check args
check_command_args ${args}

# execute command
command=$1
case ${command} in
"deploy" | "Deploy")
  module=$2
  kill_server >/dev/null 2>&1
  deploy_import_service
  ;;
"start" | "Start")
  start_import_service
  ;;
"stop" | "Stop")
  stop_import_service
  ;;
"submit" | "Submit")
  submit_job
  ;;
"clean" | "Clean")
  task_name=$2
  clean_job
  ;;
"stat" | "Stat")
  stat_job
  ;;
"retry" | "Retry")
  task_name=$2
  retry_failed_tasks
  ;;
"version" | "Version")
  version_import
  ;;
"kill" | "Kill")
  kill_server >/dev/null 2>&1
  echo "kill done"
  ;;
*)
  usage
  exit 1
  ;;
esac

exit 0
