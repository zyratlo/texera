package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.dataflow.common.Dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class FuzzyTokenizer {
    //Data members
    private static Dictionary wordBase;
    private String phrase;
    private static final String wordBaseSourceFilePath = "/queryrewriter/english_dict.csv";

    static {
        try {
            wordBase = new Dictionary(wordBaseSourceFilePath);
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
     * When called returns a single-member list containing most-likely tokenization of phrase
     * Splits phrase into terms by whitespace delimiter
     * For each term, rewrites(tokenizes) term to detect likely missing spaces
     * Concatenates all rewritten terms with singlespace delimiter
     *
     * For example, if input phrase is "newyork city", it would first split it into "newyork" and "city"
     * rewriteTerm on "newyork" should return "new york"
     * rewriteTerm on "city" should return "city"
     * The two rewritten strings are then concatenated in order by a singlespace to give "new york city"
     * The result returned as a single-member list is {"new york city"}
     *
     * @return
     */
    public List<String> getFuzzyTokens() {
        String[] terms = phrase.split("\\s+");
        String rewrittenPhrase = "";
        for(String term : terms)
            rewrittenPhrase += " " + rewriteTerm(term);
        rewrittenPhrase = rewrittenPhrase.substring(1);
        return Arrays.asList(rewrittenPhrase);
    }

    /**
     * Performs most likely tokenization of a single term
     * Uses Dynamic Programming algorithm approach used for tokenization of Chinese strings
     * Source - Chinese Tokenizer in SRCH2 codebase
     *           https://github.com/SRCH2/srch2-ngn/blob/master/src/core/analyzer/ChineseTokenizer.cpp#L197
     * @param term
     * @return
     */
    public String rewriteTerm(String term) {
        int size = term.length();
        if(size == 0)
            return "";

        double[] scoreAtGap = new double[size];
        int[] preBestGap = new int[size];

        Arrays.fill(scoreAtGap, RewriterConstants.MAXIMUM_SEQUENCE_SCORE);
        Arrays.fill(preBestGap, -1);
        scoreAtGap[0] = 0;
        preBestGap[0] = 0;

        for(int endPosition = 1; endPosition < size; endPosition++) {
            for(int spanSize = 1; spanSize < RewriterConstants.MAX_WORD_LENGTH && spanSize <= endPosition; spanSize++) {
                double freq = wordBase.getFrequency(term.substring(endPosition-spanSize, endPosition+1));
                if(freq < 0) {
                    if(spanSize == 1)
                        freq = RewriterConstants.UNKNOWN_CHAR_FREQ;
                    else
                        continue;
                }

                if(freq + scoreAtGap[endPosition-spanSize] + RewriterConstants.TOKEN_LENGTH_PENALTY < scoreAtGap[endPosition]) {
                    scoreAtGap[endPosition] = freq + scoreAtGap[endPosition-spanSize] + RewriterConstants.TOKEN_LENGTH_PENALTY;
                    preBestGap[endPosition] = endPosition - spanSize;
                }
            }
        }

        String tokenization = "";

        for(int pos = size-1; pos > 0; pos = preBestGap[pos]-1) {
            tokenization = " " + term.substring(preBestGap[pos], pos+1) + tokenization;
        }

        return tokenization.substring(1);
    }

    /**
     * When called returns array of rewritten phrases
     * Splits phrase string into delimited (by default, it is delimited using a space) terms
     * For each term performs fuzzy tokenization to create corresponding lists of rewritten terms
     * Calls method bruteRewriteTerm to perform tokenization over single term
     * Performs a cross product concatenation of each rewritten term list and returns final String array
     * Calls method crossCatenate to perform required concatenation of multiple lists
     * @return
     */
    public List<String> bruteGetFuzzyTokens() {
        String[] terms = phrase.split("\\s+");
        List< List<String> > allTermsList = new ArrayList< List <String> >();
        for(String term : terms)
            allTermsList.add(bruteRewriteTerm(term));
        return crossCatenate(allTermsList);
    }

    /**
     * Wrapper over main recursive method, bruteRewrite which performs the fuzzy tokenization of a term
     * The original term may not be a valid dictionary word, but be a term particular to the database
     * The method bruteRewrite only considers terms which are valid dictionary words
     * In case original term is not a dictionary word, it will not be added to queryList by method bruteRewrite
     * This wrapper ensures that if bruteRewrite does not include the original term, it will still be included
     * For example, for the term "newyork", bruteRewrite will return the list <"new york">
     *     But "newyork" also needs to be included in the list to support particular user queries
     *     This wrapper includes "newyork" in this list
     * @param term
     * @return
     */
    private List<String> bruteRewriteTerm(String term) {
        List<String> termsList = bruteRewrite(term);
        if(! term.equals(termsList.get(termsList.size()-1)))
            termsList.add(term);
        return termsList;
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
    private List<String> bruteRewrite(String term) {
        List<String> queryList = new ArrayList<String>();
        for(int i = 1; i < term.length(); i++) {
            String prefixString = term.substring(0, i);
            if(wordBase.contains(prefixString)) {
                prefixString = prefixString.concat(" ");

                String suffixString = term.substring(i, term.length());
                List<String> suffixList = bruteRewrite(suffixString);

                for(int j = 0; j < suffixList.size(); j++)
                    suffixList.set(j, prefixString.concat(suffixList.get(j)));

                queryList.addAll(suffixList);
            }
        }
        if(wordBase.contains(term))
            queryList.add(term);

        return queryList;
    }

    /**
     * Function to perform a cross product concatenation of separate rewritten term lists
     * For example if there are K lists: <l1>,<l2>,<l3>,...,<lK>
     *     Function creates a combined list <l> containing all possible K-combinations where
     *     each combination contains an entry from each of the K lists
     *     combinations maintain order in which terms were parsed
     * @param allWordsList
     * @return
     */
    private List<String> crossCatenate(List<List<String>> allWordsList) {
        List<String> crossList = new ArrayList<String>(allWordsList.get(0));

        for(List<String> wordList : allWordsList.subList(1, allWordsList.size())) {
            int priorCrossListLength = crossList.size();
            crossList = replicate(crossList, wordList.size());

            for(int i = 0; i < wordList.size(); i++) {
                for(int j = 0; j < priorCrossListLength; j++) {
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
        for(int i = 1; i < n; i++) {
            list.addAll(originalList);
        }
        return list;
    }
}