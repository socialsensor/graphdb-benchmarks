package eu.socialsensor.insert;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;

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
    private final BatchGraph<TitanGraph> batchGraph;

    public TitanMassiveInsertion(BatchGraph<TitanGraph> batchGraph, GraphDatabaseType type)
    {
        super(type, null /* resultsPath */); // no temp files for massive load
                                             // insert
        this.batchGraph = batchGraph;
    }

    @Override
    public Vertex getOrCreate(String value)
    {
        Integer intVal = Integer.valueOf(value);
        final long titanVertexId = TitanId.toVertexId(intVal);
        Vertex vertex = batchGraph.getVertex(titanVertexId);
        if (vertex == null)
        {
            vertex = batchGraph.addVertex(titanVertexId);
            vertex.setProperty("nodeId", intVal);
        }
        return vertex;
    }

    @Override
    public void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge("similar", dest);
    }
}
