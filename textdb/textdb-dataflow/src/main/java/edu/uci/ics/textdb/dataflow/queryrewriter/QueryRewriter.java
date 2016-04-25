package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class QueryRewriter implements IOperator{

    private String searchQuery;
    private FuzzyTokenizer fuzzyTokenizer;

    /**
     * Parameterized constructor that requires a Search Query String that
     * is to be rewritten
     * @param searchQuery
     */
    public QueryRewriter(String searchQuery) {
        this.searchQuery = searchQuery;
    }

    /**
     * Overriden Open method of the IOperator interface
     * Query parser object will be created here
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        fuzzyTokenizer = new FuzzyTokenizer(searchQuery);
    }

    /**
     * Calling appropriate fuzzyTokenizer method to populate the list
     * of rewritten search queries
     * @return - Tuple with the rewritten queries as a comma separated string
     * @throws Exception
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        return null;
    }

    /**
     * Closing the query Parser object
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        fuzzyTokenizer = null;
    }
}
