#!/bin/bash


function getmessage(){
  git log --format=%B -n 1 "$1"
}

CURRENT_COMMIT="`git rev-parse HEAD`"
CONF_FILE=conf/batch.conf
SPARK_DEFAULTS_CONF_FILE=conf/spark-defaults.conf
NRO_ATTEMPTS_PER_COMMIT=2

FROM_COMMIT="$1"
TO_COMMIT="$CURRENT_COMMIT"

if [ "$FROM_COMMIT" == "" ]
then
  echo "FROM_COMMIT parameter needed."
  exit 1
fi


LIST_OF_COMMITS_FILE="`mktemp`"

git rev-list "$FROM_COMMIT".."$TO_COMMIT" > "$LIST_OF_COMMITS_FILE"

echo "//// Starting tests..."
cat $LIST_OF_COMMITS_FILE | while read COMMIT
do

  export COMMIT_MESSAGE="`getmessage $COMMIT`"

  if [[ "$COMMIT_MESSAGE" == *"TESTME"* ]]
  then
    echo "/// Running test on commit \"$COMMIT : $COMMIT_MESSAGE\" as requested ..."

    git checkout $COMMIT

    export APP_NAME="$COMMIT_MESSAGE"
    source $CONF_FILE
    cat $SPARK_DEFAULTS_CONF_FILE.template | envsubst > $SPARK_DEFAULTS_CONF_FILE

    for attempt in `seq 1 $NRO_ATTEMPTS_PER_COMMIT`
    do
      echo "// Attempt $attempt"
      ./initialize.bash
    done

  else
    echo "/// Skipping test on commit \"$COMMIT : $COMMIT_MESSAGE\" as requested (TESTME not found)..."
  fi

  echo "////Done."

done

rm $LIST_OF_COMMITS_FILE

