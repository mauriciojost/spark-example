# README

This is a getting started project.

This is the related [Google Document](https://docs.google.com/document/d/1WabuGFxK-q99DzRwHCFCcCJoot6VrFjJtFZ3CVG7j-U/edit).

## Run

First launch the history server: 

```
cd $SPARK_HOME
./sbin/start-history-server.sh
```

Then browse: 

```
http://localhost:18080/
```

After that launch the batch test run: 
```
./batch.bash <from_commit_id>
```

### Single test run

To launch a single test run: 
```
./initialize.bash
```

See `localhost:4040` web interface.



