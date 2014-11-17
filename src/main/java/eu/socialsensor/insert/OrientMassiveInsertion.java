package eu.socialsensor.insert;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.graph.batch.OGraphBatchInsertSimple;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
public class OrientMassiveInsertion {
  protected final String url;
  protected Logger       logger = Logger.getLogger(OrientMassiveInsertion.class);

  public OrientMassiveInsertion(final String iURL) {
    url = iURL;

    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);

    OrientGraphNoTx g = new OrientGraphNoTx(url);
    for (int i = 0; i < 16; ++i) {
      g.getVertexBaseType().addCluster("v_" + i);
      g.getEdgeBaseType().addCluster("e_" + i);
    }
    g.shutdown();
  }

  public void createGraph(String datasetDir) {
    logger.setLevel(Level.INFO);
    logger.info("Loading data in massive mode in OrientDB database . . . .");

    final OGraphBatchInsertSimple g = new OGraphBatchInsertSimple(url);
    g.setAverageEdgeNumberPerNode(40);
    g.setEstimatedEntries(1000000);
    g.begin();

    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
      String line;
      int lineCounter = 1;
      long srcVertex, dstVertex;
      while ((line = reader.readLine()) != null) {
        if (lineCounter > 4) {
          String[] parts = line.split("\t");

          srcVertex = Long.parseLong(parts[0]);
          if (parts[0].equals(parts[1]))
            dstVertex = srcVertex;
          else
            dstVertex = Long.parseLong(parts[0]);

          g.createEdge(srcVertex, dstVertex);
        }
        lineCounter++;
      }
      reader.close();
      g.end();

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

}
