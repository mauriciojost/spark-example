#!/bin/bash

set -e 

NRO_ATTEMPTS_PER_COMMIT=2
LIST_OF_COMMITS_FILE="`mktemp`"
CURRENT_COMMIT="`git rev-parse HEAD`"

FROM_COMMIT="$1"
TO_COMMIT="$CURRENT_COMMIT"

if [ "$FROM_COMMIT" == "" ]
then
  echo "FROM_COMMIT parameter needed."
  exit 1
fi

function getmessage(){
  git log --format=%B -n 1 "$1"
}


git rev-list --reverse "$FROM_COMMIT".."$TO_COMMIT" > "$LIST_OF_COMMITS_FILE"

echo "//// Starting tests..."
cat $LIST_OF_COMMITS_FILE | while read COMMIT
do

  export COMMIT_MESSAGE="`getmessage $COMMIT`"

  if [[ "$COMMIT_MESSAGE" == "TESTME"* ]]
  then
    echo "/// Running test on commit \"$COMMIT : $COMMIT_MESSAGE\" as requested ..."

    git checkout $COMMIT

    for attempt in `seq 1 $NRO_ATTEMPTS_PER_COMMIT`
    do
      echo "// Attempt $attempt"
      ./run.bash
    done

  else
    echo "/// Skipping test on commit \"$COMMIT : $COMMIT_MESSAGE\" as requested (does not start with TESTME)..."
  fi

  echo "////Done."

done

rm $LIST_OF_COMMITS_FILE

