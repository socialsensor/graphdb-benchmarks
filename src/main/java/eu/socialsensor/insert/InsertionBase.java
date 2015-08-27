package eu.socialsensor.insert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.dataset.Dataset;
import eu.socialsensor.dataset.DatasetFactory;
import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

/**
 * Base class for business logic of insertion workloads
 * 
 * @author Alexander Patrikalakis
 *
 * @param <T>
 *            the Type of vertexes (graph database vendor specific)
 */
public abstract class InsertionBase<T> implements Insertion
{
    private static final Logger logger = LogManager.getLogger();
    public static final String INSERTION_CONTEXT = ".eu.socialsensor.insertion.";
    private final Timer getOrCreateTimes;
    private final Timer relateNodesTimes;

    protected final GraphDatabaseType type;
    protected final List<Double> insertionTimes;
    private final boolean single;

    // to write intermediate times for SingleInsertion subclasses
    protected final File resultsPath;

    protected InsertionBase(GraphDatabaseType type, File resultsPath)
    {
        this.type = type;
        this.insertionTimes = new ArrayList<Double>();
        this.resultsPath = resultsPath;
        this.single = resultsPath != null;
        final String insertionTypeCtxt = type.getShortname() + INSERTION_CONTEXT + (single ? "adhoc." : "batch.");
        this.getOrCreateTimes = GraphDatabaseBenchmark.metrics.timer(insertionTypeCtxt + "getOrCreate");
        this.relateNodesTimes = GraphDatabaseBenchmark.metrics.timer(insertionTypeCtxt + "relateNodes");
    }

    /**
     * Gets or creates a vertex
     * 
     * @param value
     *            the identifier of the vertex
     * @return the id of the created vertex
     */
    protected abstract T getOrCreate(final String value);

    /**
     * 
     * @param src
     * @param dest
     */
    protected abstract void relateNodes(final T src, final T dest);

    /**
     * sometimes a transaction needs to be committed at the end of a batch run.
     * this is the hook.
     */
    protected void post()
    {
        // NOOP
    }

    public final void createGraph(File datasetFile, int scenarioNumber)
    {
        logger.info("Loading data in {} mode in {} database . . . .", single ? "single" : "massive",
            type.name());
        Dataset dataset = DatasetFactory.getInstance().getDataset(datasetFile);

        T srcNode, dstNode;
        Stopwatch thousandWatch = new Stopwatch(), watch = new Stopwatch();
        thousandWatch.start();
        watch.start();
        int i = 4;
        for (List<String> line : dataset)
        {
            final Timer.Context contextSrc = getOrCreateTimes.time();
            try {
                srcNode = getOrCreate(line.get(0));
            } finally {
                contextSrc.stop();
            }

            final Timer.Context contextDest = getOrCreateTimes.time();
            try {
                dstNode = getOrCreate(line.get(1));
            } finally {
                contextDest.stop();
            }

            final Timer.Context contextRelate = relateNodesTimes.time();
            try {
                relateNodes(srcNode, dstNode);
            } finally {
                contextRelate.stop();
            }

            if (i % 1000 == 0)
            {
                insertionTimes.add((double) thousandWatch.elapsed(TimeUnit.MILLISECONDS));
                thousandWatch.stop();
                thousandWatch = new Stopwatch();
                thousandWatch.start();
            }
            i++;
        }
        post();
        insertionTimes.add((double) watch.elapsed(TimeUnit.MILLISECONDS));

        if (single)
        {
            Utils.writeTimes(insertionTimes, new File(resultsPath,
                SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_FILE_NAME_BASE + "." + type.getShortname() + "."
                    + Integer.toString(scenarioNumber)));
        }
    }
}
