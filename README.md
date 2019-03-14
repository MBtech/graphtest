# graphtest
This repository contains an example of a graph algorithm implementation in Apache Flink with skewed partitioning.
The Graph algorithm is a GSA based Single Source Shortest Path Algorithm. It's using Flink 1.7.2. 

## Usage
You can simply use the default parameters for testing but you have to pass it the test graph. 

How to compile run it with Flink:
```bash
mvn clean package
flink run target/graphtest-1.0-SNAPSHOT-jar-with-dependencies.jar <path to the input edge file>
```

I have provided a small input edge file `testgraph.txt` as an example and you can provide the absolute path to that file
