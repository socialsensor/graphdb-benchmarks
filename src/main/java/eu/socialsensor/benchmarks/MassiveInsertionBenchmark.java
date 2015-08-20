package eu.socialsensor.benchmarks;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * MassiveInsertionBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */

public class MassiveInsertionBenchmark extends PermutingBenchmarkBase implements InsertsGraphData
{
    private static final Logger logger = LogManager.getLogger();

    public MassiveInsertionBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.MASSIVE_INSERTION);
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        logger.debug("Creating database instance for type " + type.getShortname());
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        logger.debug("Prepare database instance for type {} for massive loading", type.getShortname());
        // the following step includes provisioning in managed database
        // services. do not measure this time as
        // it is not related to the action of inserting.
        graphDatabase.createGraphForMassiveLoad();
        logger.debug("Massive load graph in database type {}", type.getShortname());
        Stopwatch watch = new Stopwatch();
        watch.start();
        graphDatabase.massiveModeLoading(bench.getDataset());
        logger.debug("Shutdown massive graph in database type {}", type.getShortname());
        graphDatabase.shutdownMassiveGraph();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
