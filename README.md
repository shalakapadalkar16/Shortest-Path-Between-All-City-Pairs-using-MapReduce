# Shortest Path Between All City Pairs using MapReduce


Team
-----------
- Shalaka Padalkar
- Amod Dhopavkar

Overview
-----------
- This project calculates the shortest path between all city pairs using the MapReduce.
- The `input.csv` contains the following fields:
  - `ORIGIN`
  - `DEST`
  - `DISTANCE IN MILES`

Project Overview
-----------
The goal of this project is to efficiently compute the shortest path between all city pairs using the MapReduce. The project leverages the power of distributed computing to handle large datasets and calculate the shortest paths in a scalable manner.

Methodology
-----------
This project implements a MapReduce program to calculate the shortest paths between all city pairs in a large dataset. The program explores two approaches:

### Floyd Warshall's Algorithm
#### Job 1: Computes the Direct Shortest Paths
* **Mapper:** 
  * Reads edges and emits two key-value pairs for each edge, considering both directions.
* **Reducer:** 
  * Aggregates distances for each node pair and retains the shortest distance.
#### Job 2: Extend Paths Using Floyd-Warshall Algorithm
* **Mapper:** 
  * Maps each input record to a partition based on the source node's hash.
* **Reducer:** 
  * Processes each partition independently, computing shortest paths through intermediate nodes within the partition.

### Implicit Joins
#### Job 1: Calculate Shortest Paths for Directly Connected Cities
* **Mapper:**
  * Reads each line from the input data CSV and emits key-value pairs representing the origin city and its neighboring cities along with their distances.
* **Reducer:**
  * Combines the results and selects the shortest distance for each directly connected city pair.
#### Job 2: Extend Paths to Include 2-Hop Distances
* **Mapper:**
  * Processes the output of Job 1, which lists all directly connected pairs.
  * Emits key-value pairs where the end of one pair is the start of another, enabling the discovery of 2-hop paths.
* **Reducer:**
  * Combines the results and generates a list of city pairs with their corresponding 2-hop distances.
#### Job 3: Extend Paths to Include 3-Hop Distances
* **Mapper:**
  * Processes the output of Job 2, which contains the 2-hop pairs.
  * Emits key-value pairs where the end of one 2-hop pair is the start of another, allowing the identification of 3-hop paths.
* **Reducer:**
  * Combines the results and produces a list of city pairs with their associated 3-hop distances.
#### Job 4: Determine the Shortest Paths
* **Mapper:** 
  * Processes the outputs of Jobs 1, 2, and 3, which contain the shortest paths for directly connected cities, 2-hop paths, and 3-hop paths, respectively.
  * Emits key-value pairs with the city pair and their corresponding distances.
* **Reducer:** 
  * Combines the results from all the previous jobs and selects the minimum distance for each city pair, ensuring that the shortest path up to 3 hops is emitted.



Installation
------------
These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
  - Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

Environment
-----------
1) Example ~/.bash_aliases:
	```
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

	`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local-fw`               -- run the Floyd Warshall Algorithm
    - `make local-join`             -- run the Implicit Joins Algorithm
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws-fw`					-- Floyd Warshall: check for successful execution with web interface (aws.amazon.com)
    - `make aws-join`				-- Implicit Joins: check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination
