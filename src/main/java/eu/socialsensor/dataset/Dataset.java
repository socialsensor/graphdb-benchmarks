package eu.socialsensor.dataset;

import java.io.File;
import java.util.*;

import org.apache.commons.math3.util.MathArrays;

import eu.socialsensor.utils.Utils;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class Dataset implements Iterable<List<String>>
{
    private final List<List<String>> data;

    public Dataset(File datasetFile)
    {
        data = Utils.readTabulatedLines(datasetFile, 4 /* numberOfLinesToSkip */);
    }

    public List<Integer> generateRandomNodes(int numRandomNodes)
    {
//        Set<String> nodes = new HashSet<String>();
//        for (List<String> line : data.subList(4, data.size()))
//        {
//            for (String nodeId : line)
//            {
//                nodes.add(nodeId.trim());
//            }
//        }
//
//        List<String> nodeList = new ArrayList<String>(nodes);
//        int[] nodeIndexList = new int[nodeList.size()];
//        for (int i = 0; i < nodeList.size(); i++)
//        {
//            nodeIndexList[i] = i;
//        }
//        MathArrays.shuffle(nodeIndexList);
//
//        Set<Integer> generatedNodes = new HashSet<Integer>();
//        for (int i = 0; i < numRandomNodes; i++)
//        {
//            generatedNodes.add(Integer.valueOf(nodeList.get(nodeIndexList[i])));
//        }
        //Use old logic for now
        final int max = 1000;
        final int min = 2;
        final Random rand = new Random(17);
        final Set<Integer> generatedNodes = new HashSet<>();
        while(generatedNodes.size() < numRandomNodes + 1) { //generate one more so that we can
            generatedNodes.add(rand.nextInt((max - min) +1) + min);
        }
        return new LinkedList<>(generatedNodes);
    }

    @Override
    public Iterator<List<String>> iterator()
    {
        return data.iterator();
    }

    public List<List<String>> getList() {
        return new ArrayList<List<String>>(data);
    }
}
