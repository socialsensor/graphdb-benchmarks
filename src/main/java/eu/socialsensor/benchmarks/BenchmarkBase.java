package eu.socialsensor.benchmarks;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Base class for benchmarks.
 * 
 * @author Alexander Patrikalakis
 */
public abstract class BenchmarkBase implements Benchmark
{
    private static final Logger logger = LogManager.getLogger();
    protected final BenchmarkConfiguration bench;
    protected final File outputFile;
    protected final BenchmarkType type;

    protected BenchmarkBase(BenchmarkConfiguration bench, BenchmarkType type)
    {
        this.bench = bench;
        this.outputFile = new File(bench.getResultsPath(), type.getResultsFileName());
        this.type = type;
    }

    @Override
    public final void startBenchmark()
    {
        startBenchmarkInternal();
    }

    public abstract void startBenchmarkInternal();

    protected final void createDatabases()
    {
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            logger.info(String.format("creating %s database from %s dataset", type.getShortname(), bench.getDataset()
                .getName()));
            File dbpath = Utils.generateStorageDirectory(type, bench.getDbStorageDirectory());
            if (dbpath.exists())
            {
                throw new IllegalStateException(String.format(
                    "Database from a previous run exist: %s; clean up and try again.", dbpath.getAbsolutePath()));
            }
            Utils.createMassiveLoadDatabase(type, bench);
        }
    }

    protected final void deleteDatabases()
    {
        for (GraphDatabaseType type : bench.getSelectedDatabases())
        {
            Utils.deleteDatabase(type, bench);
        }
    }
}
