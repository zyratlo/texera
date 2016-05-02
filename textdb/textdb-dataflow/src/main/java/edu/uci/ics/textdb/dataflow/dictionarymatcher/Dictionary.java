package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import edu.uci.ics.textdb.api.common.IDictionary;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

    private Iterator<String> iterator;
    private Set<String> dictionaryWords;

    public Dictionary(Collection<String> dictionaryWords) {
        //Using LinkedHashSet so that getNextValue() returns the words in order.
        this.dictionaryWords = new LinkedHashSet<String>(dictionaryWords);
    }

    public Dictionary(String wordBaseSourceFilePath) throws IOException {
        BufferedReader wordReader = null;
        try {
            dictionaryWords = new LinkedHashSet<String>();
            String line;

            URL wordBaseURL = getClass().getResource(wordBaseSourceFilePath);
            wordReader = new BufferedReader(new FileReader(wordBaseURL.getPath()));

            while( (line = wordReader.readLine()) != null ){
                dictionaryWords.add(line);
            }
        } catch (IOException e) {
            throw e;
        } finally{
            if(wordReader != null){
                wordReader.close();
            }
        }
    }

    /**
     * Gets next value from the dictionary
     */
    @Override
    public String getNextValue() {
        if(iterator == null){
            iterator = dictionaryWords.iterator();
        }
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public boolean contains(String word) {
        return dictionaryWords.contains(word);
    }

}
