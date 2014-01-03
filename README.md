graphdb-benchmarks
==================
The project graphdb-benchmarks is a benchmark between three popular graph dataases, Titan, OrientDB and Neo4j.The purpose of this benchmark is to examine the performance of each graph database both in terms of execution time and memory consumption. The benchmark has three parts:
- *Massive Insertion Benchmark*: In this part we measure the time that each graph database needs to create the whole graph.
- *Single Insertion Benchmark*: In this part we measure the insertion time of a block, which consists of one thousand nodes and the edges that appear during the insertion of those nodes.
- *Query Benchmark*: In this part we measure the time needed to execute a query. We use a query to find the neibours of a node, a query to find the nodes of a edge and a query to find the shortest path between two nodes.

For our experiments we have used the following datasets: [Amazon dataset](http://snap.stanford.edu/data/amazon0601.html),[Youtube dataset](http://snap.stanford.edu/data/com-Youtube.html) and [LiveJournal dataset](http://snap.stanford.edu/data/com-LiveJournal.html).

Instructions
------------
To run the project firstly you should download one of the above datasets. You can download any dataset you want, but because there is not any utility class το convert the dataset in the appropriate format (for now), the format of the data must be identical with the tested datasets. Note that you probalby have to change the dataset file name in order to be identical with the defined variables. A folder named data must be created under the project's directory. This is where the dataset should be stored and where the results of the single insertion benchmark is saved. To finish the project setup you should add the extra libraries (from lib folder) to the build path.

When the setup is successfully completed open the GraphDatabaseBenchmark class, which is the main class, and uncomment the benchmark you want to run. For more details about the code, please check the comments.

Results
-------

Contact
-------
For more information or support, please contact: sotbeis@iti.gr or sot.beis@gmail.com
