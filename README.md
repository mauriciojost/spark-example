# README

This is a Spark getting started project. It mainly aims to help understanding Spark fundamentals and the impact of using different algorithms when coding Spark-based applications.

## The workflow

With this project you will be able to visualize what means using `coalesce(2)` against using `coalesce(4)` in terms of performance. 

You first clone it. Then do a commit using `groupBy`, then a commit but using `reduceBy`. When done with the versions of your app (versions that you want to compare against), then you execute `batch.bash` providing the target commits. The framework will run the application in its different versions and, once done, you will be able to compare the performances of all of them using the beautiful Spark UI portal (DAGS, memory usage, execution time, etc.).

The input data used will be automatically generated the first time you launch the application. It will remain the same for all runs.

This is the related [Google Document](https://docs.google.com/document/d/1WabuGFxK-q99DzRwHCFCcCJoot6VrFjJtFZ3CVG7j-U/edit) where I log my conclusions from some basic experiments.

## Run

To get started you need to first download [Spark](http://spark.apache.org/downloads.html) (v1.4 or older). Then launch its history server: 

```
cd $SPARK_HOME
./sbin/start-history-server.sh
```

Here you will be able to visualize the performances of the execution of different versions of your application. It will allow you to compare Spark performances as you change your algorithms.

The history server can be browsed here: 

```
http://localhost:18080/
```

The history server will not show anything at the beginning. Now you can launch your tests using a range of commit: from a specified commit (through it's GIT commit ID) until now, using only the commits whose message contain the string `TESTME`.  Launch the batch test: 

```
cd $THIS_APP_HOME
./batch.bash <from_commit_id>
```

### Single test run

To launch a single test run (for debugging purposes): 
```
cd $THIS_APP_HOME
./run.bash
```

See `localhost:4040` web interface.


