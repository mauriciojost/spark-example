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

- Try to use primitive types as much as possible.
