package eu.socialsensor.insert;

import java.io.File;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.util.TitanId;

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
        Integer intVal = Integer.valueOf(value);
        final long titanVertexId = TitanId.toVertexId(intVal);
        final GraphTraversal<Vertex, Vertex> t = graph.traversal().V(T.id, titanVertexId);
        final Vertex vertex = t.hasNext() ? t.next() : graph.addVertex(T.label, NODE_LABEL,
                                                                       T.id, titanVertexId,
                                                                       NODEID, intVal);
        graph.tx().commit();
        return vertex;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
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
}
