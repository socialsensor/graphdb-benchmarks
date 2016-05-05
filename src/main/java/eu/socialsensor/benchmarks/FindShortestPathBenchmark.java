package eu.socialsensor.benchmarks;

import eu.socialsensor.dataset.DatasetFactory;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * FindShortestPathBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class FindShortestPathBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{

    public FindShortestPathBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.FIND_SHORTEST_PATH);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type, false /*batchLoading*/);
        Stopwatch watch = Stopwatch.createStarted();
        graphDatabase.shortestPaths();
        graphDatabase.shutdown();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
