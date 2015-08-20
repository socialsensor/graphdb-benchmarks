package eu.socialsensor.benchmarks;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Benchmark that measures the time requried to delete a graph
 * @author Alexander Patrikalakis
 *
 */
public class DeleteGraphBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{
    public DeleteGraphBenchmark(BenchmarkConfiguration bench)
    {
        super(bench, BenchmarkType.DELETION);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        Stopwatch watch = new Stopwatch();
        watch.start();
        Utils.deleteDatabase(type, bench);
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
