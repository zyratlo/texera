package edu.uci.ics.textdb.dataflow.queryrewriter;

/**
 * Created by kishorenarendran on 25/04/16.
 * Class to take in a query and with a dictionary of words split it into possible set of queries
 * Gets called by QueryRewriter
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

