package eu.socialsensor.query;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
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

  private OrientExtendedGraph orientGraph = null;

  public OrientQuery(OrientExtendedGraph orientGraph) {
    this.orientGraph = orientGraph;
  }

  public static void main(String args[]) {
  }

  @Override
  public void findNeighborsOfAllNodes() {
    for (Vertex v : orientGraph.getVertices()) {
      for (Vertex vv : v.getVertices(Direction.BOTH, "similar")) {
      }
    }
  }

  @Override
  public void findNodesOfAllEdges() {
    for (Vertex v : orientGraph.getVertices()) {
      for (Vertex vv : v.getVertices(Direction.BOTH)) {
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
    }

  }
}
