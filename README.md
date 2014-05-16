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
We list some of the results from our evaluation. The first table depicts the results of MIW and WQ with the Livejournal dataset.

| Dataset | Workload | Titan | OrientDB | Neo4j |
| ------- | -------- | ----- | -------- | ----- |
|   LJ    |    MIW   |720.179|2485.529  |**229.465**|
|   LJ    |   QW-FN  |352.968|**68.176**|109.494|
|   LJ    |   QW-FA  |504.909|341.306   |**44.510**|
|   LJ    |   QW-FS  |31.01  |47.183    |0.479  |



Contact
-------
For more information or support, please contact: sotbeis@iti.gr or sot.beis@gmail.com
