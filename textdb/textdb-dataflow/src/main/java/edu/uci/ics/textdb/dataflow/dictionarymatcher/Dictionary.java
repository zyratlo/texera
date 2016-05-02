package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import edu.uci.ics.textdb.api.common.IDictionary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

    private int cursor = -1;
    private List<String> stringList;
    private HashSet<String> stringHashSet;

    public Dictionary(List<String> stringList) {
        this.stringHashSet = new HashSet<String>(stringList);
        this.stringList = stringList;
        cursor = 0;
    }

    public Dictionary(String wordBaseSourceFilePath) throws IOException {
        stringHashSet = new HashSet<String>();
        String line;

        URL wordBaseURL = getClass().getResource(wordBaseSourceFilePath);
        BufferedReader wordReader = new BufferedReader(new FileReader(wordBaseURL.getPath()));

        while( (line = wordReader.readLine()) != null )
            stringHashSet.add(line);

        stringList = new ArrayList<String>(stringHashSet);
        cursor = 0;
    }

    /**
     * Gets next value from the dictionary
     */
    @Override
    public String getNextValue() {
        if (cursor >= stringList.size()) {
            return null;
        }
        String dictval = stringList.get(cursor++);
        return dictval;
    }

    public boolean contains(String word) {
        return stringHashSet.contains(word);
    }

}
