package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import org.apache.lucene.search.Query;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class QueryRewriter implements IOperator{

    private Query searchQuery;
    private QueryParser queryParser;

    /**
     * Parameterized constructor that requires a Lucene Search Query that
     * is to be rewritten
     * @param searchQuery
     */
    public QueryRewriter(Query searchQuery) {
        this.searchQuery = searchQuery;
    }

    /**
     * Overriden Open method of the IOperator interface
     * Query parser object will be created here
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        queryParser = new QueryParser(searchQuery.toString());
    }

    /**
     * Calling appropriate queryParser method to populate the list
     * of rewritten search queries
     * @return
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
        queryParser = null;
    }
}
