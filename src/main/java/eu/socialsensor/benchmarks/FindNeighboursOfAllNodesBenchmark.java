package eu.socialsensor.benchmarks;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * FindNeighboursOfAllNodesBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class FindNeighboursOfAllNodesBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{
    public FindNeighboursOfAllNodesBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.FIND_NEIGHBOURS);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open();
        Stopwatch watch = Stopwatch.createStarted();        
        graphDatabase.findAllNodeNeighbours();
        graphDatabase.shutdown();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
