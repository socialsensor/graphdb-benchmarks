package eu.socialsensor.insert;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.log4j.Level;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
  * Implementation of massive Insertion in OrientDB graph database
  * 
  * @author sotbeis
  * @email sotbeis@iti.gr
  * 
  */
public class OrientMassiveInsertion extends OrientAbstractInsertion {

  public OrientMassiveInsertion(OrientBaseGraph orientGraph, OIndex vertices) {
    super(orientGraph, vertices);
  }

  @Override
  public void createGraph(String datasetDir) {
    logger.setLevel(Level.INFO);
    logger.info("Loading data in massive mode in OrientDB database . . . .");
    orientGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
      String line;
      int lineCounter = 1;
      OrientVertex srcVertex, dstVertex;
      Iterable<OrientVertex> cache;
      while ((line = reader.readLine()) != null) {
        if (lineCounter > 4) {
          String[] parts = line.split("\t");
          final Integer key = Integer.parseInt(parts[0]);
          cache = vertexIndexLookup(key);
          if (cache.iterator().hasNext()) {
            srcVertex = cache.iterator().next();
          } else {
            srcVertex = orientGraph.addVertex(null, "nodeId", key);
            vertices.put(key, srcVertex);
          }

          final Integer key2 = Integer.parseInt(parts[1]);
          cache = vertexIndexLookup(key2);
          if (cache.iterator().hasNext()) {
            dstVertex = cache.iterator().next();
          } else {
            dstVertex = orientGraph.addVertex(null, "nodeId", key2);
            vertices.put(key2, dstVertex);
          }

          if (key.equals(key2)) {
            dstVertex = srcVertex;
          }

          orientGraph.addEdge(null, srcVertex, dstVertex, "similar");
        }
        lineCounter++;

        if (lineCounter % 1000 == 0)
          orientGraph.commit();

      }
      orientGraph.commit();
      reader.close();
      orientGraph.getRawGraph().declareIntent(null);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

}
