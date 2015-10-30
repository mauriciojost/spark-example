# README

This is a getting started project.

## Run

## Debug

See `localhost:4040` web interface.

## Tricks

- If having OOM during shuffles then probably it is required to increase the parallelism level: 

```
--conf spark.default.parallelism=4
```

This configuration controls the amount of reducers in a group by.
The amount of splits is controlled by the amount of lines in the initial RDD, unless otherwise specified.

For instance if using `spark.default.parallelism=2` we obtain in the second stage (shuffle) the following:

```
Spark assembly has been built with Hive, including Datanucleus jars on classpath
15/10/30 08:02:34 WARN Utils: Your hostname, tuca resolves to a loopback address: 127.0.1.1; using 192.168.43.127 instead (on interface wlan0)
15/10/30 08:02:34 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
15/10/30 08:02:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
[Stage 1:>                                                                                                 (0 + 2) / 2]

```

But using `spark.default.parallelism=4` we obtain:

```
Spark assembly has been built with Hive, including Datanucleus jars on classpath
15/10/30 08:02:34 WARN Utils: Your hostname, tuca resolves to a loopback address: 127.0.1.1; using 192.168.43.127 instead (on interface wlan0)
15/10/30 08:02:34 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
15/10/30 08:02:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
[Stage 1:>                                                                                                 (0 + x) / 4]

```

- Try to use primitive types as much as possible.

