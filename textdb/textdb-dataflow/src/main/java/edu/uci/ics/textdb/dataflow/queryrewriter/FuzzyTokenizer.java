package edu.uci.ics.textdb.dataflow.queryrewriter;

/**
 * Created by kishorenarendran on 25/04/16.
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
        //TODO - Stubbed function
        return new String[]{this.query};
    }
}

