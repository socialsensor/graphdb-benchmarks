package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.main.GraphDatabaseType;

public abstract class GraphDatabaseBase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType> implements GraphDatabase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType>
{
    private static final Logger LOG = LogManager.getLogger();
    public static final String SIMILAR = "similar";
    public static final String QUERY_CONTEXT = ".eu.socialsensor.query.";
    public static final String NODE_ID = "nodeId";
    public static final String NODE_COMMUNITY = "nodeCommunity";
    public static final String COMMUNITY = "community";
    public static final String NODE_LABEL = "node";
    protected final File dbStorageDirectory;
    protected final MetricRegistry metrics = new MetricRegistry();
    protected final GraphDatabaseType type;
    private final Timer nextVertexTimes;
    private final Timer getNeighborsOfVertexTimes;
    private final Timer nextEdgeTimes;
    private final Timer getOtherVertexFromEdgeTimes;
    private final Timer getAllEdgesTimes;
    private final Timer shortestPathTimes;

    protected GraphDatabaseBase(GraphDatabaseType type, File dbStorageDirectory)
    {
        this.type = type;
        final String queryTypeContext = type.getShortname() + QUERY_CONTEXT;
        this.nextVertexTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "nextVertex");
        this.getNeighborsOfVertexTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "getNeighborsOfVertex");
        this.nextEdgeTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "nextEdge");
        this.getOtherVertexFromEdgeTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "getOtherVertexFromEdge");
        this.getAllEdgesTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "getAllEdges");
        this.shortestPathTimes = GraphDatabaseBenchmark.metrics.timer(queryTypeContext + "shortestPath");
        
        this.dbStorageDirectory = dbStorageDirectory;
        if (!this.dbStorageDirectory.exists())
        {
            this.dbStorageDirectory.mkdirs();
        }
    }
    
    @Override
    public void findAllNodeNeighbours() {
        long nodeDegreeSum = 0;
        VertexIteratorType vertexIterator =  this.getVertexIterator();
        while(vertexIteratorHasNext(vertexIterator)) {
            VertexType vertex;
            Timer.Context ctxt = nextVertexTimes.time();
            try {
                vertex = nextVertex(vertexIterator);
            } finally {
                ctxt.stop();
            }

            final EdgeIteratorType edgeNeighborIterator;
            ctxt = getNeighborsOfVertexTimes.time();
            try {
                //gets forward and reverse edges.
                edgeNeighborIterator = this.getNeighborsOfVertex(vertex);
            } finally {
                ctxt.stop();
            }
            while(edgeIteratorHasNext(edgeNeighborIterator)) {
                EdgeType edge;
                ctxt = nextEdgeTimes.time();
                try {
                    edge = nextEdge(edgeNeighborIterator);
                } finally {
                    ctxt.stop();
                }
                @SuppressWarnings("unused")
                Object other;
                ctxt = getOtherVertexFromEdgeTimes.time();
                try {
                    other = getOtherVertexFromEdge(edge, vertex);
                } finally {
                    ctxt.stop();
                }
                nodeDegreeSum++;
            }
            this.cleanupEdgeIterator(edgeNeighborIterator);
        }
        this.cleanupVertexIterator(vertexIterator);
        LOG.debug("The sum of node degrees was " + nodeDegreeSum);
    }
    
    @Override
    public void findNodesOfAllEdges() {
        int edges = 0;
        EdgeIteratorType edgeIterator;
        Timer.Context ctxt = getAllEdgesTimes.time();
        try {
            edgeIterator = this.getAllEdges();
        } finally {
            ctxt.stop();
        }

        while(edgeIteratorHasNext(edgeIterator)) {
            EdgeType edge;
            ctxt = nextEdgeTimes.time();
            try {
                edge = nextEdge(edgeIterator);
            } finally {
                ctxt.stop();
            }
            @SuppressWarnings("unused")
            VertexType source = this.getSrcVertexFromEdge(edge);
            @SuppressWarnings("unused")
            VertexType destination = this.getDestVertexFromEdge(edge);
            edges++;
        }
        LOG.debug("Counted " + edges + " edges");
    }
    
    @Override
    public void shortestPaths(Set<Integer> nodes) {
        //randomness of selected node comes from the hashing function of hash set
        final Iterator<Integer> it = nodes.iterator();
        Preconditions.checkArgument(it.hasNext());
        final VertexType from = getVertex(it.next());
        it.remove();//now the set has 99 nodes
        Timer.Context ctxt;
        for(Integer i : nodes) {
            //time this
            ctxt = shortestPathTimes.time();
            try {
                shortestPath(from, i);
            } finally {
                ctxt.stop();
            }
        }
    }
}
