package eu.socialsensor.graphdatabases;

import java.io.File;
import java.util.Set;

import org.neo4j.graphdb.Transaction;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import eu.socialsensor.main.GraphDatabaseBenchmark;
import eu.socialsensor.main.GraphDatabaseType;

@SuppressWarnings("deprecation")
public abstract class GraphDatabaseBase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType> implements GraphDatabase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType>
{
    public static final String SIMILAR = "similar";
    public static final String QUERY_CONTEXT = ".eu.socialsensor.query.";
    public static final String NODE_ID = "nodeId";
    public static final String NODE_COMMUNITY = "nodeCommunity";
    public static final String COMMUNITY = "community";
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
        //get the iterator
        Object tx = null;
        if(GraphDatabaseType.NEO4J == type) { //TODO fix this
            tx = ((Neo4jGraphDatabase) this).neo4jGraph.beginTx();
        }
        try {
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
                }
                this.cleanupEdgeIterator(edgeNeighborIterator);
            }
            this.cleanupVertexIterator(vertexIterator);
            if(this instanceof Neo4jGraphDatabase) {
                ((Transaction) tx).success();
            }
        } finally {//TODO fix this
            if(GraphDatabaseType.NEO4J == type) {
                ((Transaction) tx).finish();
            }
        }
    }
    
    @Override
    public void findNodesOfAllEdges() {
        Transaction tx = null;
        if(GraphDatabaseType.NEO4J == type) {//TODO fix this
            tx = ((Neo4jGraphDatabase) this).neo4jGraph.beginTx();
        }
        try {
            
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
            }
        } finally {//TODO fix this
            if(GraphDatabaseType.NEO4J == type) {
                tx.close();
            }
        }
    }
    
    @Override
    public void shortestPaths(Set<Integer> nodes) {
        Object tx = null;
        if(GraphDatabaseType.NEO4J == type) {//TODO fix this
            tx = ((Neo4jGraphDatabase) this).neo4jGraph.beginTx();
        }
        try {
            //TODO(amcp) change this to use 100+1 random node list and then to use a sublist instead of always choosing node # 1
            VertexType from = getVertex(1);
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
            if(this instanceof Neo4jGraphDatabase) {
                ((Transaction) tx).success();
            }
        } finally {//TODO fix this
            if(GraphDatabaseType.NEO4J == type) {
                ((Transaction) tx).finish();
            }
        }
    }
}
