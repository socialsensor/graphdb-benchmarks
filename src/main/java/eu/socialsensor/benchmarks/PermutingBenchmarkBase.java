package eu.socialsensor.benchmarks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.iterators.PermutationIterator;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Base class abstracting the logic of permutations
 * 
 * @author Alexander Patrikalakis
 */
public abstract class PermutingBenchmarkBase extends BenchmarkBase
{
    protected final Map<GraphDatabaseType, List<Double>> times;
    private static final Logger LOG = LogManager.getLogger();

    protected PermutingBenchmarkBase(BenchmarkConfiguration bench, BenchmarkType typeIn)
    {
        super(bench, typeIn);
        times = new HashMap<GraphDatabaseType, List<Double>>();
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            times.put(type, new ArrayList<Double>(bench.getScenarios()));
        }
    }

    @Override
    public void startBenchmarkInternal()
    {
        LOG.info(String.format("Executing %s Benchmark . . . .", type.longname()));

        if (bench.permuteBenchmarks())
        {
            PermutationIterator<GraphDatabaseType> iter = new PermutationIterator<GraphDatabaseType>(
                bench.getSelectedDatabases());
            int cntPermutations = 1;
            while (iter.hasNext())
            {
                LOG.info("Scenario " + cntPermutations);
                startBenchmarkInternalOnePermutation(iter.next(), cntPermutations);
                cntPermutations++;
            }
        }
        else
        {
            startBenchmarkInternalOnePermutation(bench.getSelectedDatabases(), 1);
        }

        LOG.info(String.format("%s Benchmark finished", type.longname()));
        post();
    }

    private void startBenchmarkInternalOnePermutation(Collection<GraphDatabaseType> types, int cntPermutations)
    {
        for (GraphDatabaseType type : types)
        {
            benchmarkOne(type, cntPermutations);
        }
    }

    public abstract void benchmarkOne(GraphDatabaseType type, int scenarioNumber);

    public void post()
    {
        Utils.writeResults(outputFile, times, type.longname());
    }
}
