package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created by shiladitya on 4/25/16.
 */
public class WordDictionary {

    private static HashSet<String> wordDict;

    public WordDictionary(String dictSourceFileName) throws IOException {
        wordDict = new HashSet<String>();
        BufferedReader wordReader = new BufferedReader(new FileReader(dictSourceFileName));
        String line;

        while( (line = wordReader.readLine()) != null )
            wordDict.add(line);
    }

    public boolean contains(String word) {
        return wordDict.contains(word);
    }
}