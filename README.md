graphdb-benchmarks
==================
The project graphdb-benchmarks is a benchmark between three popular graph dataases, Titan, OrientDB and Neo4j.The purpose of this benchmark is to examine the performance of each graph database both in terms of execution time and memory consumption. The benchmark is composed of four workloads, Clustering, Massive Insertion, Single Insertion and Query Workload. Every workload has been designed to simulate common operations in graph database systems

- *Clustering Workload (CW)*: CW consists of a well-known community detection algorithm for modularity optimization, the Louvain Method. We adapt the algorithm on top of the benchmarked graph databases and employ cache techniques to take advantage of both graph database capabilities and in-memory execution speed. We measure the time the algorithm needs to converge.
- *Massive Insertion Workload (MIW)*: we create the graph database and configure it for massive loading, then we populate it with a particular dataset. We measure the time for the creation of the whole graph.
- *Single Insertion Workload (SIW)*: we create the graph database and load it with a particular dataset. Every object insertion (node or edge) is committed directly and the graph is constructed incrementally. We measure the insertion time per block, which consists of one thousand nodes and the edges that appear during the insertion of these nodes.
- *Query Workload (QW)*: we execute three common queries:
  * FindNeighbours (FN): finds the neighbours of all nodes.
  * FindAdjacentNodes (FA): finds the adjacent nodes of all edges.
  * FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

Here we measure the execution time of each query.

For our evaluation we use both synthetic and real data. More specifically, we execute MIW, SIW and QW with real data  derived from the SNAP dataset collection ([Enron Dataset](http://snap.stanford.edu/data/email-Enron.html), [Amazon dataset](http://snap.stanford.edu/data/amazon0601.html), [Youtube dataset](http://snap.stanford.edu/data/com-Youtube.html) and [LiveJournal dataset](http://snap.stanford.edu/data/com-LiveJournal.html)). On the other hand, with the CW we use synthetic data generated with the [LFR-Benchmark generator](https://sites.google.com/site/andrealancichinetti/files) which produces networks with power-law
degree distribution and implanted communities within the network.

Instructions
------------
To run the project firstly you should download one of the above datasets. You can download any dataset you want, but because there is not any utility class το convert the dataset in the appropriate format (for now), the format of the data must be identical with the tested datasets. From the config/input.properties file you should choose the dataset (aslo the dataset path) and the workload you want to run. Moreover you should specify the path you want to write the results. For the CW the cache values should be specified from the properties file. When the configuration is done open the GraphDatabaseBenchmark class, which is the main class and run the benchmark. For more details about the code, please check the comments in the code.

Results
-------
Below we list the results of MIW and QW for each dataset. The time is measured in seconds.

| Dataset | Workload | Titan | OrientDB | Neo4j    |
| ------- | -------- | ----- | -------- | -----    |
|   EN    |    MIW   |9.36   |62.77     |**6.77**  |
|   AM    |    MIW   |34.00  |97.00     |**10.61** |
|   YT    |    MIW   |104.27 |252.15    |**24.69** |
|   LJ    |    MIW   |663.03 |9416.74   |**349.55**|
|                                                  |
|   EN    |  QW-FN   |1.87   |**0.56**  |0.95  |
|   AM    |  QW-FN   |6.47   |3.50      |**1.85**  |
|   YT    |  QW-FN   |20.71  |9.34      |**4.51**  |
|   LJ    |  QW-FN   |213.41 |303.09    |**47.07** |
|                                                  |
|   EN    |  QW-FA   |3.78   |0.71     |**0.16**  |
|   AM    |  QW-FA   |13.77  |2.30     |**0.36**  |
|   YT    |  QW-FA   |42.82  |6.15     |**1.46**  |
|   LJ    |  QW-FA   |460.25 |518.12   |**16.53** |
|                                                  |
|   EN    |  QW-FS   |1.63   |3.09      |**0.16**  |
|   AM    |  QW-FS   |0.12   |83.29     |**0.302** |
|   YT    |  QW-FS   |24.87  |23.47     |**0.08**  |
|   LJ    |  QW-FS   |123.50 |86.87     |**18.13** |

Below we list the results of the CW for graphs with 1,000, 5,000 and 10,0000 nodes. Here the time is also measured in seconds.

| Graph-Cache   | Titan | OrientDB | Neo4j |
| -----------   | ----- | -------- | ----- |
|Graph1000-5%   |2.49   |**0.91**  |2.88   |
|Graph1000-10%  |1.48   |**0.61**  |2.12   |
|Graph1000-15%  |1.35   |**0.57**  |2.03   |
|Graph1000-20%  |1.32   |**0.52**  |1.91   |
|Graph1000-25%  |1.30   |**0.50**  |1.69   |
|                                          |
|Graph5000-5%   |16.62  |**5.85**  |14.06  |
|Graph5000-10%  |15.84  |**5.63**  |13.18  |
|Graph5000-15%  |15.15  |**4.78**  |12.96  |
|Graph5000-20%  |14.24  |**4.51**  |12.89  |
|Graph5000-25%  |14.10  |**4.60**  |12.19  |
|                                          |
|Graph10000-5%  |49.45  |**18.26** |37.37  |
|Graph10000-10% |46.97  |**17.73** |35.50  |
|Graph10000-15% |47.84  |**17.47** |34.70  |
|Graph10000-20% |44.86  |**17.03** |37.62  |
|Graph10000-25% |44.01  |**16.87** |33.18  |
|                                          |
|Graph50000-5%  |694.18 |**269.07**|372.42 |
|Graph50000-10% |677.30 |**256.19**|370.36 |
|Graph50000-15% |665.26 |**252.62**|364.51 |
|Graph50000-20% |664.10 |**251.84**|353.68 |
|Graph50000-25% |643.50 |**248.51**|348.81 |

Contact
-------
For more information or support, please contact: sotbeis@iti.gr or sot.beis@gmail.com
