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

    @Override
    public Iterator<List<String>> iterator()
    {
        return data.iterator();
    }

    public List<List<String>> getList() {
        return new ArrayList<List<String>>(data);
    }
}
