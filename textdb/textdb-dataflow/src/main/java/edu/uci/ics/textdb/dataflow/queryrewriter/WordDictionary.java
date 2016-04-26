package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created by shiladitya on 4/25/16.
 * Class to provide simple abstraction of a dictionary of words with constant retrieval time
 * Used by FuzzyTokenizer to process query
 */
public class WordDictionary {

    private HashSet<String> wordDictionary;

    public WordDictionary(String dictSourceFileName) throws IOException {
        wordDictionary = new HashSet<String>();
        BufferedReader wordReader = new BufferedReader(new FileReader(dictSourceFileName));
        String line;

        while( (line = wordReader.readLine()) != null )
            wordDictionary.add(line);
    }

    public boolean contains(String word) {
        return wordDictionary.contains(word);
    }
}