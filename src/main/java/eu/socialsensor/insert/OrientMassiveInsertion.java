package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class OrientMassiveInsertion extends InsertionBase<Vertex> implements Insertion
{
    private final Graph graph;

    public OrientMassiveInsertion(Graph graph)
    {
        super(GraphDatabaseType.ORIENT_DB, null /* resultsPath */);
        this.graph = graph;
    }

    @Override
    protected Vertex getOrCreate(String value)
    {
        final Integer intValue = Integer.valueOf(value);
        final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().hasLabel(NODE_LABEL).has(NODEID, intValue);
        final Vertex vertex = traversal.hasNext() ? traversal.next() : graph.addVertex(T.label, OrientGraphDatabase.NODE_LABEL, NODEID, intValue);
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(SIMILAR, dest);
    }

    @Override
    protected void post()
    {
        graph.tx().commit();
    }
}
