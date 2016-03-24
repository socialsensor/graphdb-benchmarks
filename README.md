graphdb-benchmarks
==================
The project graphdb-benchmarks is a benchmark between popular graph databases. Currently the framework supports [Titan](http://thinkaurelius.github.io/titan/), [OrientDB](http://www.orientechnologies.com/orientdb/), and [Neo4j](http://neo4j.com/). The purpose of this benchmark is to examine the performance of each graph database in terms of execution time. The benchmark is composed of four workloads, Clustering, Massive Insertion, Single Insertion and Query Workload. Every workload has been designed to simulate common operations in graph database systems.

- *Clustering Workload (CW)*: CW consists of a well-known community detection algorithm for modularity optimization, the Louvain Method. We adapt the algorithm on top of the benchmarked graph databases and employ cache techniques to take advantage of both graph database capabilities and in-memory execution speed. We measure the time the algorithm needs to converge.
- *Massive Insertion Workload (MIW)*: we create the graph database and configure it for massive loading, then we populate it with a particular data set. We measure the time for the creation of the whole graph.
- *Single Insertion Workload (SIW)*: we create the graph database and load it with a particular data set. Every object insertion (node or edge) is committed directly and the graph is constructed incrementally. We measure the insertion time per block, which consists of one thousand edges and the nodes that appear during the insertion of these edges.
- *Query Workload (QW)*: we execute three common queries:
  * FindNeighbours (FN): finds the neighbors of all nodes.
  * FindAdjacentNodes (FA): finds the adjacent nodes of all edges.
  * FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

Here we measure the execution time of each query.

For our evaluation we use both synthetic and real data. More specifically, we execute MIW, SIW and QW with real data derived from the SNAP data set collection ([Enron data set](http://snap.stanford.edu/data/email-Enron.html), [Amazon data set](http://snap.stanford.edu/data/amazon0601.html), [Youtube data set](http://snap.stanford.edu/data/com-Youtube.html) and [LiveJournal data set](http://snap.stanford.edu/data/com-LiveJournal.html)). On the other hand, with the CW we use synthetic data generated with the [LFR-Benchmark generator](https://sites.google.com/site/andrealancichinetti/files) which produces networks with power-law degree distribution and implanted communities within the network. The synthetic data can be downloaded from [here](http://figshare.com/articles/Synthetic_Data_for_graphdb_benchmark/1221760).

For further information about the study please refer to the [published paper](http://link.springer.com/chapter/10.1007/978-3-319-10518-5_1) on Springer site and the presentation on [Slideshare](http://www.slideshare.net/sympapadopoulos/adbis2014-presentation).

**Note 1:** The published paper contains the experimental study of Titan, OrientDB and Neo4j. After the publication we included the Sparksee graph database. Sparksee does not implement TinkerPop 3 yet.

**Note 2:** After the very useful comments and contributions of OrientDB developers, we updated the benchmark implementations and re-run the experiments. We have updated the initial presentation with the new results and uploaded a new version of the paper in the following [link](http://mklab.iti.gr/files/beis_adbis2014_corrected.pdf).

**Note 3:** Alexander Patrikalakis, a software developer at Amazon Web Services, refactored the benchmark, added support for Apache TinkerPop 3 and added support for the DynamoDB Storage Backend for Titan.

Instructions
------------
To run the project at first you have to choose one of the aforementioned data sets. Of course you can select any data set, but because there is not any utility class to convert the data set in the appropriate format (for now), the format of the data must be identical with the tested data sets. The input parameters are configured from the src/test/resources/input.properties file. Please follow the instructions in this file to select the correct parameters. Then, run `mvn install && mvn test -Pbench` to execute the benchmarking run.

Results
-------
This section contains the results of each benchmark. All the measurements are in seconds.


####CW results
Below we list the results of the CW for graphs with 1,000, 5,000, 10,0000, 20,000, 30,000, 40,000, 50,000 nodes.

| Graph-Cache | Titan | OrientDB | Neo4j |
| ----------- | ----- | -------- | ----- |
|Graph1k-5%   |2.39   |**0.92**  |2.46   |
|Graph1k-10%  |1.45   |**0.59**  |2.07   |
|Graph1k-15%  |1.30   |**0.58**  |1.88   |
|Graph1k-20%  |1.25   |**0.55**  |1.72   |
|Graph1k-25%  |1.19   |**0.49**  |1.67   |
|Graph1k-30%  |1.15   |**0.48**  |1.55   |
|                                        |
|Graph5k-5%   |16.01  |**5.88**  |12.80  |
|Graph5k-10%  |15.10  |**5.67**  |12.13  |
|Graph5k-15%  |14.63  |**4.81**  |11.91  |
|Graph5k-20%  |14.16  |**4.62**  |11.68  |
|Graph5k-25%  |13.76  |**4.51**  |11.31  |
|Graph5k-30%  |13.38  |**4.45**  |10.94  |
|                                        |
|Graph10k-5%  |46.06  |**18.20** |34.05  |
|Graph10k-10% |44.59  |**17.92** |32.88  |
|Graph10k-15% |43.68  |**17.31** |31.91  |
|Graph10k-20% |42.48  |**16.88** |31.01  |
|Graph10k-25% |41.32  |**16.58** |30.74  |
|Graph10k-30% |39.98  |**16.34** |30.13  |
|                                        | 
|Graph20k-5%  |140.46 |**54.01** |87.04  |
|Graph20k-10% |138.10 |**52.51** |85.49  |
|Graph20k-15% |137.25 |**52.12** |82.88  |
|Graph20k-20% |133.11 |**51.68** |82.16  |
|Graph20k-25% |122.48 |**50.79** |79.87  |
|Graph20k-30% |120.94 |**50.49** |78.81  |
|                                        |
|Graph30k-5%  |310.25 |**96.38** |154.60 |
|Graph30k-10% |301.80 |**94.98** |151.81 |
|Graph30k-15% |299.27 |**94.85** |151.12 |
|Graph30k-20% |296.43 |**94.67** |146.25 |
|Graph30k-25% |294.33 |**92.62** |144.08 |
|Graph30k-30% |288.50 |**90.13** |142.33 |
|                                        |
|Graph40k-5%  |533.29 |**201.19**|250.79 |
|Graph40k-10% |505.91 |**199.18**|244.79 |
|Graph40k-15% |490.39 |**194.34**|242.55 |
|Graph40k-20% |478.31 |**183.14**|241.47 |
|Graph40k-25% |467.18 |**177.55**|237.29 |
|Graph40k-30% |418.07 |**174.65**|229.65 |
|                                        |
|Graph50k-5%  |642.42 |**240.58**|348.33 |
|Graph50k-10% |624.36 |**238.35**|344.06 |
|Graph50k-15% |611.70 |**237.65**|340.20 |
|Graph50k-20% |610.40 |**230.76**|337.36 |
|Graph50k-25% |596.29 |**230.03**|332.01 |
|Graph50k-30% |580.44 |**226.31**|325.88 |


####MIW & QW results
Below we list the results of MIW and QW for each dataset.
The results are measured in seconds.

| Dataset | Workload | Titan-BDB  | Titan-Tupl | Neo4j       |
| ------- | -------- | ---------- | ---------- | ----------- |
|   1k    |  QW-FA   |     0.331  |     0.104  |   **0.043** |
|   5k    |  QW-FA   |     2.235  |     0.645  |   **0.203** |
|  10k    |  QW-FA   |     5.059  |     1.182  |   **0.389** |
|   EN    |  QW-FA   |     5.842  |     1.653  |   **0.403** |
|  20k    |  QW-FA   |    10.568  |     2.521  |   **0.826** |
|  30k    |  QW-FA   |    18.356  |     4.638  |   **1.383** |
|  40k    |  QW-FA   |    27.907  |     7.107  |   **2.010** |
|  50k    |  QW-FA   |    34.284  |     9.716  |   **2.472** |
|   AM    |  QW-FA   |    61.811  |    19.015  |   **3.413** |
|         |          |            |            |             |
|   1k    |  QW-FN   |     0.607  |     0.229  |   **0.131** |
|   5k    |  QW-FN   |     2.850  |     0.964  |   **0.626** |
|  10k    |  QW-FN   |     5.960  |     2.063  |   **1.349** |
|   EN    |  QW-FN   |     7.711  |     3.915  |   **1.633** |
|  20k    |  QW-FN   |    12.861  |     5.218  |   **2.841** |
|  30k    |  QW-FN   |    21.816  |     8.340  |   **4.603** |
|  40k    |  QW-FN   |    31.187  |    11.632  |   **7.272** |
|  50k    |  QW-FN   |    41.175  |    14.246  |   **8.489** |
|   AM    |  QW-FN   |    76.562  |    28.242  |  **12.466** |
|         |          |            |            |             |
|   1k    |    MIW   |     1.167  |     0.673  |   **0.481** |
|   5k    |    MIW   |     4.276  |     2.918  |   **1.239** |
|  10k    |    MIW   |     8.247  |     5.659  |   **2.334** |
|   EN    |    MIW   |     9.858  |     6.960  |   **2.401** |
|  20k    |    MIW   |    17.011  |    12.711  |   **4.511** |
|  30k    |    MIW   |    30.252  |    19.929  |   **8.767** |
|  40k    |    MIW   |    44.450  |    31.763  |  **12.761** |
|  50k    |    MIW   |    57.001  |    36.281  |  **15.755** |
|   AM    |    MIW   |    98.405  |    64.286  |  **23.867** |

Note, Find Shortest Path benchmark is currently broken.
Consequently, I did not update the QW-FS numbers.
Also, OrientDB's TP3 implementation is not official yet,
so I did not run numbers for OrientDB as well.
These benchmarks were performed on the RAM disk (/dev/shm) of a m4.10xlarge with a maximum heap size of 32 GiB.

I also analyzed storage the footprint of each of these databases.
I conclude that the storage footprint in MiB for all of them is linear with respect
to the number of vertices and edges.

| Dataset | Vertices | Edges   | Titan-BDB | Titan-Tupl | Neo4j     |
| ------- | -------- | ------- | --------- | ---------- | --------- |
|   1k    |    1000  |   15160 |     1.7   |   **0.9**  |     1.0   |
|   5k    |    5000  |  148198 |    16.0   |     7.5    |   **5.7** |
|  10k    |   10000  |  360632 |    38.4   |    18.3    |  **13.1** |
|   EN    |   36692  |  367666 |    43.8   |    21.4    |  **15.7** |
|  20k    |   20000  |  778900 |    83.8   |    42.0    |  **27.7** |
|  30k    |   30000  | 1332020 |   145.4   |    73.8    |  **46.8** |
|  40k    |   40000  | 2013894 |   221.2   |   111.3    |  **70.2** |
|  50k    |   50000  | 2512092 |   277.0   |   138.1    |  **87.3** |
|   AM    |  403394  | 3387392 |   441.2   |   213.3    | **147.4** |

Applying least squares optimization to this multitude of data points yields the following estimates of space
in bytes required to store each vertex and edge (assuming no labels and no properties).

| Bytes on disk | Titan-BDB | Titan-Tupl | Neo4j     |
| ------------- | --------- | ---------- | --------- |
| Per vertex    | 0.212     | **0.082**  |   0.090   |
| Per edge      | 0.108     |   0.055    | **0.034** |

####SIW results
Below we list the results of SIW for each data set.

![siw_benchmark_updated](https://cloud.githubusercontent.com/assets/8163869/12272282/62b1c9f4-b914-11e5-85be-efd3f58e1e05.png)
<!---
![alt text](https://raw.githubusercontent.com/socialsensor/graphdb-benchmarks/master/images/SIWEnron.png "Logo Title Text 1")
![alt text](https://raw.githubusercontent.com/socialsensor/graphdb-benchmarks/master/images/SIWAmazon.png "Logo2 Title Text 1")
![alt text](https://raw.githubusercontent.com/socialsensor/graphdb-benchmarks/master/images/SIWYoutube.png "Logo Title Text 1")
![alt text](https://raw.githubusercontent.com/socialsensor/graphdb-benchmarks/master/images/SIWLivejournal.png "Logo4 Title Text 1")
--->

Contact
-------
For more information or support, please contact: sotbeis@iti.gr, sot.beis@gmail.com, papadop@iti.gr or amcp@me.com.
