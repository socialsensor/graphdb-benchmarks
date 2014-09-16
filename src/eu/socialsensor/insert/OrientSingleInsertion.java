package eu.socialsensor.insert;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;
import org.apache.log4j.Level;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of single Insertion in OrientDB graph database
 * 
 * @author sotbeis
 * @email sotbeis@iti.gr
 * 
 */
public class OrientSingleInsertion extends OrientAbstractInsertion {
  private static int count;

  public OrientSingleInsertion(OrientExtendedGraph orientGraph) {
    super(orientGraph);
  }

  @Override
  public void createGraph(String datasetDir) {
    INSERTION_TIMES_OUTPUT_PATH = SingleInsertionBenchmark.INSERTION_TIMES_OUTPUT_PATH + ".orient";

    count++;

    logger.setLevel(Level.INFO);
    logger.info("Incrementally loading data in Orient database . . . .");
    List<Double> insertionTimes = new ArrayList<Double>();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
      String line;
      int lineCounter = 1;
      Vertex srcVertex, dstVertex;
      int edgeCounter = 0;
      long start = System.currentTimeMillis();
      long duration;
      while ((line = reader.readLine()) != null) {
        if (lineCounter > 4) {
          String[] parts = line.split("\t");

          srcVertex = getOrCreate(parts[0]);
          dstVertex = getOrCreate(parts[1]);

          orientGraph.addEdge(null, srcVertex, dstVertex, "similar");

          if (orientGraph instanceof TransactionalGraph) {
        	  ((TransactionalGraph) orientGraph).commit();
          }
          
          if (lineCounter % 1000 == 0) {
              duration = System.currentTimeMillis() - start;
              insertionTimes.add((double) duration);
              edgeCounter = 0;
              start = System.currentTimeMillis();
            }
          
        }
        lineCounter++;
      }

      if (orientGraph instanceof TransactionalGraph) {
    	  ((TransactionalGraph) orientGraph).commit();
      }

      duration = System.currentTimeMillis() - start;
      insertionTimes.add((double) duration);
      
      reader.close();
    } 
    catch (IOException ioe) {
      System.out.println(ioe);
      ioe.printStackTrace();
    } 
    catch (Exception e) {
      System.out.println(e);
    }
    Utils utils = new Utils();
    utils.writeTimes(insertionTimes, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH + "." + count);
  }
}