package eu.socialsensor.insert;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.asynch.OrientGraphAsynch;
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

  public OrientMassiveInsertion(OrientExtendedGraph orientGraph) {
    super(orientGraph);

    if (orientGraph instanceof OrientGraphAsynch)
      ((OrientGraphAsynch) orientGraph).execute(new OCallable<Object, OrientBaseGraph>() {
        @Override
        public Object call(OrientBaseGraph iArgument) {
          for (int i = 0; i < 16; ++i) {
            iArgument.getVertexBaseType().addCluster("v_" + i);
            iArgument.getEdgeBaseType().addCluster("e_" + i);
          }
          return null;
        }
      });
    else {
      OrientGraphNoTx g = new OrientGraphNoTx(orientGraph.getRawGraph().getURL());
      for (int i = 0; i < 16; ++i) {
        g.getVertexBaseType().addCluster("v_" + i);
        g.getEdgeBaseType().addCluster("e_" + i);
      }
      g.shutdown();
      ODatabaseRecordThreadLocal.INSTANCE.set(orientGraph.getRawGraph());
    }

  }

  @Override
  public void createGraph(String datasetDir) {
    logger.setLevel(Level.INFO);
    logger.info("Loading data in massive mode in OrientDB database . . . .");
    orientGraph.declareIntent(new OIntentMassiveInsert());
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
      String line;
      int lineCounter = 1;
      Vertex srcVertex, dstVertex;
      while ((line = reader.readLine()) != null) {
        if (lineCounter > 4) {
          String[] parts = line.split("\t");

          srcVertex = getOrCreate(parts[0]);
          if (parts[0].equals(parts[1]))
            dstVertex = srcVertex;
          else
            dstVertex = getOrCreate(parts[1]);

          orientGraph.addEdge(null, srcVertex, dstVertex, "similar");
        }
        lineCounter++;

        if (orientGraph instanceof TransactionalGraph && lineCounter % 2000 == 0)
          ((TransactionalGraph) orientGraph).commit();
      }

      if (orientGraph instanceof TransactionalGraph)
        ((TransactionalGraph) orientGraph).commit();

      reader.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

}
