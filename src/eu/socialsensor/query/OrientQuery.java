package eu.socialsensor.query;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import eu.socialsensor.benchmarks.FindShortestPathBenchmark;

import java.util.List;

/**
 * Query implementation for OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 */
public class OrientQuery implements Query {

  private OrientBaseGraph orientGraph = null;

  public OrientQuery(OrientBaseGraph orientGraph) {
    this.orientGraph = orientGraph;
  }

  public static void main(String args[]) {
  }

  @Override
  public void findNeighborsOfAllNodes() {
    for (Vertex v : orientGraph.getVertices()) {
      for (Vertex vv : v.getVertices(Direction.BOTH, "similar")) {

      }
      // GremlinPipeline<String, Vertex> getNeighboursPipe = new GremlinPipeline<String, Vertex>(v).both("similar");
    }
  }

  @Override
  public void findNodesOfAllEdges() {
    for (Vertex v : orientGraph.getVertices()) {
      for (Vertex vv : v.getVertices(Direction.BOTH)) {
        // GremlinPipeline<String, Vertex> getNodesPipe = new GremlinPipeline<String, Vertex>(e).bothV();
        // Iterator<Vertex> vertexIter = getNodesPipe.iterator();
        // @SuppressWarnings("unused")
        // Vertex startNode = vertexIter.next();
        // @SuppressWarnings("unused")
        // Vertex endNode = vertexIter.next();
      }
    }
  }

  @Override
  public void findShortestPaths() {
    Vertex v1 = orientGraph.getVertices("nodeId", "1").iterator().next();
    for (int i : FindShortestPathBenchmark.generatedNodes) {
      final Vertex v2 = orientGraph.getVertices("nodeId", String.valueOf(i)).iterator().next();

      List<OrientVertex> result = orientGraph
          .command(new OCommandSQL("SELECT shortestPath(" + v1.getId() + "," + v2.getId() + ")")).execute();
      int length = result.size();

      // @SuppressWarnings("rawtypes")
      // final GremlinPipeline<String, List> pathPipe = new GremlinPipeline<String, List>(v1)
      // .as("similar")
      // .out("similar")
      // .loop("similar", new PipeFunction<LoopBundle<Vertex>, Boolean>() {
      // //@Override
      // public Boolean compute(LoopBundle<Vertex> bundle) {
      // return bundle.getLoops() < 5 && !bundle.getObject().equals(v2);
      // }
      // })
      // .path();
      // @SuppressWarnings("unused")
      // int length = pathPipe.iterator().next().size();
    }

  }
}
