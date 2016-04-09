package eu.socialsensor.graphdatabases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.function.Predicate;

/**
 * Depth predicate for shortest path
 *
 * @author Alexander Patrikalakis
 */
public class DepthPredicate implements Predicate<Traverser<T>> {
    private static final Logger LOG = LogManager.getLogger();
    private final int hops;

    public DepthPredicate(int hops) {
        this.hops = hops;
    }

    @Override
    public boolean test(Traverser<T> it) {
        LOG.trace("testing {}", it.path());
        return it.path().size() <= hops;
    }
}
