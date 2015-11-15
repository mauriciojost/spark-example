#!/bin/bash


function getmessage(){
  git log --format=%B -n 1 "$1"
}


CURRENT_COMMIT="`git rev-parse HEAD`"
CONF_FILE=conf/batch.conf
DEFAULTS_SPARK_CONF_FILE=conf/defaults-spark.conf

FROM_COMMIT="$1"
TO_COMMIT="$CURRENT_COMMIT"


LIST_OF_COMMITS_FILE="`mktemp`"

git rev-list "$FROM_COMMIT" "$TO_COMMIT" > "$LIST_OF_COMMITS_FILE"

cat $LIST_OF_COMMITS_FILE > while read COMMIT
do

  export COMMIT_MESSAGE="`getmessage $COMMIT`"
  echo "### Running test on commit $COMMIT : $COMMIT_MESSAGE"

  export APP_NAME="$COMMIT_MESSAGE"
  source $CONF_FILE
  cat $DEFAULTS_SPARK_CONF_FILE.template | envsubst > $DEFAULTS_SPARK_CONF_FILE

  ./initialize.bash

  echo "Done."

done

rm $LIST_OF_COMMITS_FILE

