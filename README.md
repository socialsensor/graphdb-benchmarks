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

| Dataset | Workload | Titan-BDB (new)  | Neo4j (new) |
| ------- | -------- | ---------------- | ----------- |
|   EN    |  QW-FA   |     5.235        |   **0.311** |
|   AM    |  QW-FA   |  __13.770__      |     1.730   |
|   YT    |  QW-FA   |  __42.820__      |     1.512   |
|   LJ    |  QW-FA   | __460.250__      |  __16.530__ |
|   1k    |  QW-FA   |     0.348        |   **0.031** |
|   5k    |  QW-FA   |     1.935        |   **0.115** |
|  10k    |  QW-FA   |     4.056        |   **0.225** |
|  20k    |  QW-FA   |    10.212        |   **0.590** |
|  30k    |  QW-FA   |    20.108        |   **0.793** |
|  40k    |  QW-FA   |    27.879        |   **1.145** |
|  50k    |  QW-FA   |    35.398        |   **1.376** |
|         |          |                  |             |
|   EN    |  QW-FN   |     6.411        |   **0.720** |
|   AM    |  QW-FN   |   __6.470__      |     5.788   |
|   YT    |  QW-FN   |  __20.710__      |     5.567   |
|   LJ    |  QW-FN   | __213.410__      |  __47.070__ |
|   1k    |  QW-FN   |     0.614        |   **0.103** |
|   5k    |  QW-FN   |     2.548        |   **0.329** |
|  10k    |  QW-FN   |     5.643        |   **0.636** |
|  20k    |  QW-FN   |    12.717        |   **1.333** |
|  30k    |  QW-FN   |    21.015        |   **2.290** |
|  40k    |  QW-FN   |    32.035        |   **3.708** |
|  50k    |  QW-FN   |    42.97         |   **6.465** |
|         |          |                  |             |
|   EN    |    MIW   |     9.514        |   **1.970** |
|   AM    |    MIW   |  __34.000__      |    16.464   |
|   YT    |    MIW   | __104.270__      |    19.810   |
|   LJ    |    MIW   | __663.030__      | __349.550__ |
|   1k    |    MIW   |     1.207        |   **0.525** |
|   5k    |    MIW   |     3.701        |   **1.235** |
|  10k    |    MIW   |     7.520        |   **1.821** |
|  20k    |    MIW   |    15.816        |   **3.580** |
|  30k    |    MIW   |    29.423        |   **5.912** |
|  40k    |    MIW   |    44.702        |   **9.097** |
|  50k    |    MIW   |    57.315        |  **11.687** |

Note, Find Shortest Path benchmark is currently broken.
Consequently, I did not update the QW-FS numbers.
Also, OrientDB's TP3 implementation is not official yet,
so I did not run numbers for OrientDB as well.
Finally, bold numbers indicate the fastest performer and italics indicate
tests that have not been run for updated results yet.

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
