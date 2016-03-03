package eu.socialsensor.insert;

import java.io.File;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import eu.socialsensor.graphdatabases.OrientGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public final class OrientSingleInsertion extends InsertionBase<Vertex>
{
    protected final Graph graph;

    public OrientSingleInsertion(Graph graph, File resultsPath)
    {
        super(GraphDatabaseType.ORIENT_DB, resultsPath);
        this.graph = graph;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        try
        {
            src.addEdge(SIMILAR, dest);
            graph.tx().commit();
        }
        catch (Exception e)
        {
            graph.tx().rollback();
        }
    }

    protected Vertex getOrCreate(final String value) {
        final Integer intValue = Integer.valueOf(value);
        final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(NODEID, intValue);
        final Vertex vertex = traversal.hasNext() ? traversal.next() : graph.addVertex(OrientGraphDatabase.NODE_LABEL);
        vertex.property(NODEID, intValue);
        graph.tx().commit();
        return vertex;
    }

}
