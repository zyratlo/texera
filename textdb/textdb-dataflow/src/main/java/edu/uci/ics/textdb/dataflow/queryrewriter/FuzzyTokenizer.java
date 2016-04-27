package edu.uci.ics.textdb.dataflow.queryrewriter;

/**
 * Created by kishorenarendran on 25/04/16.
 * Class to take in a string and uses a dictionary of words, to split it into possible
 * set of strings, with appropriate space delimitions. his is a resource used by the QueryRewriter
 * operator
 *
 *
 * For example - "newyorkcity" - > ["new york city", "newyorkcity"]
 * For example - "horseshoe" -> ["hor se shoe", "hors es hoe", "horse shoe", "horses hoe", "horseshoe"]
 */
public class FuzzyTokenizer {

    private String query;

    /**
     * Constructor that accepts the string version of the Lucene search query
     *
     * @param query
     */
    public FuzzyTokenizer(String query) {
        this.query = query;
    }

    /**
     * Function that returns the list of parsed/rewritten queries
     *
     * @return
     */
    public String[] getParsedQueries() {
        //TODO - Stubbed function - Fill in actual logic
        String[] result = new String[1];
        result[0] = new String(query);
        return result;
    }
}

