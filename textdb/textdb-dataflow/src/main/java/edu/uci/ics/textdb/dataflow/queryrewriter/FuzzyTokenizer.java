package edu.uci.ics.textdb.dataflow.queryrewriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shiladityasen on 4/30/16.
 * Brute force algorithm to take input a phrase string and output an array of strings as rewritten phrases
 * A phrase is a string that may comprise of multiple space separated strings called terms
 * Algorithm tries to find a valid prefix of a term and parses the rest of the term recursively
 * After recursive call returns with a set of parsed strings, prefix is appended in front of each string
 *
 * For example -> ["newyorkcity"] -> ["new york city", "newyorkcity"]
 * Here, "new york city" is a returned phrase because "new", "york" and "city" are valid words in the word base.
 */
public class FuzzyTokenizer
{
    //Data members
    private static WordBase wordBase;
    private String phrase;

    static {
        try {
            wordBase = new WordBase("wordsEn.txt");
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parameterized constructor requires phrase string that is to be tokenized
     * Creates wordBase object containing knowledge base of words in file wordsEn.txt
     * @param phrase
     * @throws IOException
     */
    public FuzzyTokenizer(String phrase) {
        this.phrase = phrase;
    }

    /**
     * When called returns array of rewritten phrases
     * Splits phrase string into delimited (by default, it is delimited using a space) terms
     * For each term performs fuzzy tokenization to create corresponding lists of rewritten terms
     * Calls method rewriteTerm to perform tokenization over single term
     * Performs a cross product concatenation of each rewritten term list and returns final String array
     * Calls method crossCatenate to perform required concatenation of multiple lists
     * @return
     */
    public List<String> getFuzzyTokens() {
        String[] queryWords = phrase.split("\\s+");
        List< List<String> > allWordsList = new ArrayList< List <String> >();
        for(String word : queryWords)
            allWordsList.add(rewriteTerm(word));
        return crossCatenate(allWordsList);
    }

    /**
     * Wrapper over main recursive method, rewrite which performs the fuzzy tokenization of a term
     * Ensures that original phrase term is also included in returned rewritten terms list
     * Is necessary if original phrase term is not a valid word according to word knowledge base
     * @param term
     * @return
     */
    private List<String> rewriteTerm(String term) {
        List<String> queryWordList = rewrite(term);
        if(! term.equals(queryWordList.get(queryWordList.size()-1)))
            queryWordList.add(term);
        return queryWordList;
    }

    /**
     * Main recursive algorithm to find possible phrase strings that original phrase term can be mapped to
     * Assumes original phrase term may have a tokenization mistake due to missing multiple space delimiters
     * Tries to find all possible valid prefixes of original term as per word knowledge base
     * For each prefix, sends rest of string to a recursive call to get all possible rewritten suffixes
     * Attaches prefix term in front of each rewritten suffix in the list
     * Returns final phrase term list
     * @param term
     * @return
     */
    private List<String> rewrite(String term) {
        List<String> queryList = new ArrayList<String>();
        for(int i=1; i<=term.length(); i++) {
            if(i != term.length()) {
                String prefixString = term.substring(0, i);
                if(wordBase.contains(prefixString)) {
                    prefixString = prefixString.concat(" ");

                    String suffixString = term.substring(i, term.length());
                    List<String> suffixList = rewrite(suffixString);

                    for(int j=0; j<suffixList.size(); j++)
                        suffixList.set(j, prefixString.concat(suffixList.get(j)));

                    queryList.addAll(suffixList);
                }
            }
            else {
                if(wordBase.contains(term))
                    queryList.add(term);
            }
        }
        return queryList;
    }

    /**
     * Function to perform a cross product concatenation of separate rewritten term lists
     * For example if there are K lists: <l1>,<l2>,<l3>,...,<lK>
     *     Function creates a combined list <l> containing all possible K-combinations where
     *     each combination contains an entry from each of the K lists
     * @param allWordsList
     * @return
     */
    private List<String> crossCatenate(List<List<String>> allWordsList) {
        List<String> crossList = new ArrayList<String>(allWordsList.get(0));

        for(List<String> wordList : allWordsList.subList(1, allWordsList.size())) {
            int priorCrossListLength = crossList.size();
            crossList = replicate(crossList, wordList.size());

            for(int i=0; i<wordList.size(); i++) {
                for(int j=0; j<priorCrossListLength; j++) {
                    int index = i*priorCrossListLength + j;
                    crossList.set(index, crossList.get(index)+" "+wordList.get(i));
                }
            }
        }

        return crossList;
    }

    /**
     * Function that takes in a list and a number n and returns a list that is input list, repeated n times
     * @param list
     * @param n
     * @return
     */
    private List<String> replicate(List<String> list, int n) {
        List<String> originalList = new ArrayList<>(list);
        for(int i=1; i<n; i++) {
            list.addAll(originalList);
        }
        return list;
    }
}
