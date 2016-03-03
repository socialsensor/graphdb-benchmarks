package eu.socialsensor.insert;

import java.io.File;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanSingleInsertion extends InsertionBase<Vertex>
{
    private final Graph graph;

    public TitanSingleInsertion(Graph titanGraph, GraphDatabaseType type, File resultsPath)
    {
        super(type, resultsPath);
        this.graph = titanGraph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        final Transaction tx = graph.tx();
        final Integer intValue = Integer.valueOf(value);
        final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(NODEID, intValue);
        final Vertex vertex = traversal.hasNext() ? traversal.next() : graph.addVertex(NODEID, intValue);
        tx.commit();
        return vertex;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        final Transaction tx = graph.tx();
        try
        {
            src.addEdge(SIMILAR, dest);
            tx.commit();
        }
        catch (Exception e)
        {
            tx.rollback();
        }
    }
}
