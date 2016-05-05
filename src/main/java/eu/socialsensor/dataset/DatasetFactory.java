package eu.socialsensor.dataset;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class DatasetFactory
{
    private static DatasetFactory theInstance = null;
    private final Map<File, Dataset> datasetMap;

    private DatasetFactory()
    {
        datasetMap = new HashMap<File, Dataset>();
    }

    public static DatasetFactory getInstance()
    {
        if (theInstance == null)
        {
            theInstance = new DatasetFactory();
        }
        return theInstance;
    }

    public Dataset getDataset(File datasetFile) {
        if (!datasetMap.containsKey(datasetFile))
        {
            throw new IllegalArgumentException("no mapping for data file " + datasetFile.getAbsolutePath());
        }
        return datasetMap.get(datasetFile);
    }
    public Dataset createAndGetDataset(File datasetFile, Random random, int randomNodeSetSize)
    {
        if (!datasetMap.containsKey(datasetFile))
        {
            datasetMap.put(datasetFile, new Dataset(datasetFile, random, randomNodeSetSize));
        }

        return datasetMap.get(datasetFile);
    }
}
