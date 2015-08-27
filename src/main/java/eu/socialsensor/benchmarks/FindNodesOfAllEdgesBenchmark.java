package eu.socialsensor.benchmarks;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * FindNodesOfAllEdgesBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class FindNodesOfAllEdgesBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{
    public FindNodesOfAllEdgesBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.FIND_ADJACENT_NODES);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open();
        Stopwatch watch = new Stopwatch();
        watch.start();
        graphDatabase.findNodesOfAllEdges();
        graphDatabase.shutdown();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
