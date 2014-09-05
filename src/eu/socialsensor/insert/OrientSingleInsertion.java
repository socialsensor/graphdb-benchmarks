package eu.socialsensor.insert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;

import eu.socialsensor.benchmarks.SingleInsertionBenchmark;
import eu.socialsensor.utils.Utils;

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

      long start = System.currentTimeMillis();
      long duration;
      while ((line = reader.readLine()) != null) {
        if (lineCounter > 4) {
          String[] parts = line.split("\t");

          srcVertex = getOrCreate(parts[0]);

          if (nodesCounter == 1000) {
            duration = System.currentTimeMillis() - start;
            insertionTimes.add((double) duration);
            nodesCounter = 0;
            start = System.currentTimeMillis();
          }

          dstVertex = getOrCreate(parts[1]);

          orientGraph.addEdge(null, srcVertex, dstVertex, "similar");

          if (nodesCounter == 1000) {
            duration = System.currentTimeMillis() - start;
            insertionTimes.add((double) duration);
            nodesCounter = 0;
            start = System.currentTimeMillis();
          }
        }
        lineCounter++;
      }

      duration = System.currentTimeMillis() - start;
      insertionTimes.add((double) duration);
      reader.close();
    } catch (IOException ioe) {
      System.out.println(ioe);
      ioe.printStackTrace();
    } catch (Exception e) {
      System.out.println(e);
    }
    Utils utils = new Utils();
    utils.writeTimes(insertionTimes, OrientSingleInsertion.INSERTION_TIMES_OUTPUT_PATH + "." + count);
  }
}
