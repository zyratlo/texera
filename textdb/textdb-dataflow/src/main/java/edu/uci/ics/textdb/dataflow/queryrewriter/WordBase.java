package edu.uci.ics.textdb.dataflow.queryrewriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by shiladityasen on 4/30/16.
 * Class to provide simple abstraction of a dictionary of words with constant retrieval time
 * Used by FuzzyTokenizer to process query
 */
public class WordBase
{

    private HashSet<String> wordBase;

    public WordBase(String wordBaseSourceFilePath) throws IOException {
        wordBase = new HashSet<String>();
        String line;

        URL wordBaseURL = getClass().getResource(wordBaseSourceFilePath);
        System.out.println(wordBaseURL.getPath());

        BufferedReader wordReader = new BufferedReader(new FileReader(wordBaseURL.getPath()));

        while( (line = wordReader.readLine()) != null )
            wordBase.add(line);
    }

    public boolean contains(String word) {
        return wordBase.contains(word);
    }
}
