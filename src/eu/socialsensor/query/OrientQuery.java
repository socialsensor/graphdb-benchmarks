package eu.socialsensor.query;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.graph.sql.functions.OSQLFunctionShortestPath;
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
    for (int k = 0; k < 3; ++k) {
      final long start = System.currentTimeMillis();

      OrientVertex v1 = (OrientVertex) orientGraph.getVertices("nodeId", 1).iterator().next();
      for (int i : FindShortestPathBenchmark.generatedNodes) {
        final OrientVertex v2 = (OrientVertex) orientGraph.getVertices("nodeId", i).iterator().next();

        List<OIdentifiable> result = (List<OIdentifiable>) new OSQLFunctionShortestPath().execute(orientGraph, null, null,
            new Object[] { v1.getRecord(), v2.getRecord(), Direction.OUT, 5 }, new OBasicCommandContext());

        int length = result.size();

        System.out.printf("\nORIENTDB SP(%s,%s): %d", 1, i, length);
      }

      System.out.printf("\nORIENTDB TOTAL: " + (System.currentTimeMillis() - start));
    }
  }
}
