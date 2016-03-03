package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of massive Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanMassiveInsertion extends InsertionBase<Vertex>
{
    private final Graph graph;

    public TitanMassiveInsertion(Graph graph, GraphDatabaseType type)
    {
        super(type, null /* resultsPath */); // no temp files for massive load
                                             // insert
        this.graph = graph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        Integer intVal = Integer.valueOf(value);
        final GraphTraversal<Vertex, Vertex> t = graph.traversal().V().has(NODEID, intVal);
        final Vertex vertex = t.hasNext() ? t.next() : graph.addVertex(NODEID, intVal);
        return vertex;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(SIMILAR, dest);
    }

    @Override
    protected void post() {
        graph.tx().commit();
    }
}
