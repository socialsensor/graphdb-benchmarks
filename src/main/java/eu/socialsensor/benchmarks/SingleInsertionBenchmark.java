package eu.socialsensor.benchmarks;

import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * SingleInsertionBenchmak implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class SingleInsertionBenchmark extends PermutingBenchmarkBase implements InsertsGraphData
{
    public static final String INSERTION_TIMES_OUTPUT_FILE_NAME_BASE = "SINGLE_INSERTIONResults";
    private static final Logger LOG = LogManager.getLogger();

    public SingleInsertionBenchmark(BenchmarkConfiguration bench)
    {
        super(bench, BenchmarkType.SINGLE_INSERTION);
    }

    @Override
    public void post()
    {
        LOG.info("Write results to " + outputFile.getAbsolutePath());
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            String prefix = outputFile.getParentFile().getAbsolutePath() + File.separator
                + INSERTION_TIMES_OUTPUT_FILE_NAME_BASE + "." + type.getShortname();
            List<List<Double>> insertionTimesOfEachScenario = Utils.getDocumentsAs2dList(prefix, bench.getScenarios());
            times.put(type, Utils.calculateMeanList(insertionTimesOfEachScenario));
            Utils.deleteMultipleFiles(prefix, bench.getScenarios());
        }
        // use the logic of the superclass method after populating the times map
        super.post();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.createGraphForSingleLoad();
        graphDatabase.singleModeLoading(bench.getDataset(), bench.getResultsPath(), scenarioNumber);
        graphDatabase.shutdown();
    }
}
