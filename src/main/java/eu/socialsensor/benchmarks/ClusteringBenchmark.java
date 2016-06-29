package eu.socialsensor.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Stopwatch;

import eu.socialsensor.clustering.LouvainMethod;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Metrics;
import eu.socialsensor.utils.Utils;

/**
 * ClusteringBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class ClusteringBenchmark extends BenchmarkBase implements RequiresGraphData
{
    private static final Logger LOG = LogManager.getLogger();
    private final List<Integer> cacheValues;

    public ClusteringBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.CLUSTERING);
        this.cacheValues = new ArrayList<Integer>();
        if (config.getCacheValues() == null)
        {
            int cacheValueMultiplier = config.getCacheIncrementFactor().intValue() * config.getNodesCount();
            for (int i = 1; i <= config.getCacheValuesCount(); i++)
            {
                cacheValues.add(i * cacheValueMultiplier);
            }
        }
        else
        {
            cacheValues.addAll(config.getCacheValues());
        }
    }

    @Override
    public void startBenchmarkInternal()
    {
        LOG.info("Executing Clustering Benchmark . . . .");
        SortedMap<GraphDatabaseType, Map<Integer, Double>> typeTimesMap = new TreeMap<GraphDatabaseType, Map<Integer, Double>>();
        try
        {
            for (GraphDatabaseType type : bench.getSelectedDatabases())
            {
                typeTimesMap.put(type, clusteringBenchmark(type));
            }
        }
        catch (ExecutionException e)
        {
            throw new BenchmarkingException("Unable to run clustering benchmark: " + e.getMessage(), e);
        }

        try (BufferedWriter out = new BufferedWriter(new FileWriter(outputFile)))
        {
            out.write("DB,Cache Size (measured in nodes),Clustering Benchmark Time (s)\n");
            for (GraphDatabaseType type : bench.getSelectedDatabases())
            {
                for (Integer cacheSize : typeTimesMap.get(type).keySet())
                {
                    out.write(String.format("%s,%d,%f\n", type.getShortname(), cacheSize,
                        typeTimesMap.get(type).get(cacheSize)));
                }
            }
        }
        catch (IOException e)
        {
            throw new BenchmarkingException("Unable to write clustering results to file");
        }
        LOG.info("Clustering Benchmark finished");
    }

    private SortedMap<Integer, Double> clusteringBenchmark(GraphDatabaseType type) throws ExecutionException
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open();

        SortedMap<Integer, Double> timeMap = new TreeMap<Integer, Double>();
        for (int cacheSize : cacheValues)
        {
            LOG.info("Graph Database: " + type.getShortname() + ", Dataset: " + bench.getDataset().getName()
                + ", Cache Size: " + cacheSize);

            Stopwatch watch = new Stopwatch();
            watch.start();
            LouvainMethod louvainMethodCache = new LouvainMethod(graphDatabase, cacheSize, bench.randomizedClustering());
            louvainMethodCache.computeModularity();
            timeMap.put(cacheSize, watch.elapsed(TimeUnit.MILLISECONDS) / 1000.0);

            // evaluation with NMI
            Map<Integer, List<Integer>> predictedCommunities = graphDatabase.mapCommunities(louvainMethodCache.getN());
            Map<Integer, List<Integer>> actualCommunities = mapNodesToCommunities(Utils.readTabulatedLines(
                bench.getActualCommunitiesFile(), 4 /* numberOfLinesToSkip */));
            Metrics metrics = new Metrics();
            double NMI = metrics.normalizedMutualInformation(bench.getNodesCount(), actualCommunities,
                predictedCommunities);
            LOG.info("NMI value: " + NMI);
        }
        graphDatabase.shutdown();
        return timeMap;
    }

    private static Map<Integer, List<Integer>> mapNodesToCommunities(List<List<String>> tabulatedLines)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
        // http://figshare.com/articles/Synthetic_Data_for_graphdb_benchmark/1221760
        // the format of the communityNNNN.dat files have node and community
        // number separated by a tab.
        // community number starts at 1 and not zero.
        for (List<String> line : tabulatedLines)
        {
            int node = Integer.valueOf(line.get(0));
            int community = Integer.valueOf(line.get(1).trim()) - 1;
            if (!communities.containsKey(community))
            {
                communities.put(community, new ArrayList<Integer>());
            }
            communities.get(community).add(node);
        }
        return communities;
    }
}
