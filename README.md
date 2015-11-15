# README

This is a getting started project.
It aims to understand Spark fundamentals by executing the application in its different commit versions
over the same set of data, to later compare performances. No need to generate the data, if it is not 
present it will be automatically generated the first time.

This is the related 
[Google Document](https://docs.google.com/document/d/1WabuGFxK-q99DzRwHCFCcCJoot6VrFjJtFZ3CVG7j-U/edit) 
where I log my conclusions from these basic experiments.

## Run

To get started you need to first launch the history server: 

```
cd $SPARK_HOME
./sbin/start-history-server.sh
```

Here you will be able to visualize the performances of the execution of different versions of your 
application. It will allow you to compare Spark performances as you change your algorithms.

The history server can be browsed here: 

```
http://localhost:18080/
```

The history server will not show anything at the beginning. Now you can launch your tests using
a range of commit: from a specified commit (through it's GIT commit ID) until now, using only 
the commits whose message contain the string `TESTME`.  Launch the batch test: 

```
./batch.bash <from_commit_id>
```

### Single test run

To launch a single test run (for debugging purposes): 
```
./run.bash
```

See `localhost:4040` web interface.



