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
    private final List<Integer> generatedNodes;

    public Dataset(File datasetFile, Random random, int randomNodeSetSize)
    {
        data = Utils.readTabulatedLines(datasetFile, 4 /* numberOfLinesToSkip */);
        final Set<Integer> nodes = new HashSet<>();
        //read node strings and convert to Integers and add to HashSet
        data.stream().forEach(line -> { //TODO evaluate parallelStream
            line.stream().forEach(nodeId -> {
                nodes.add(Integer.valueOf(nodeId.trim()));
            });
        });
        if(randomNodeSetSize > nodes.size()) {
            throw new IllegalArgumentException("cant select more random nodes than there are unique nodes in dataset");
        }

        //shuffle
        final List<Integer> nodeList = new ArrayList<>(nodes);
        Collections.shuffle(nodeList);

        //choose randomNodeSetSize of them
        generatedNodes = new ArrayList<Integer>(randomNodeSetSize);
        Iterator<Integer> it = nodeList.iterator();
        while(generatedNodes.size() < randomNodeSetSize) {
            generatedNodes.add(it.next());
        }
    }

    @Override
    public Iterator<List<String>> iterator()
    {
        return data.iterator();
    }

    public List<List<String>> getList() {
        return new ArrayList<List<String>>(data);
    }
    public List<Integer> getRandomNodes() {
        return generatedNodes;
    }
}
