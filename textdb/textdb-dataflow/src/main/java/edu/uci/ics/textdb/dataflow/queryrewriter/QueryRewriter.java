package edu.uci.ics.textdb.dataflow.queryrewriter;

import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;

/**
 * Created by kishorenarendran on 25/04/16.
 * QueryRewriter is an operator that converts a query search string which
 * has faults in space placement to a list of meaningful search queries
 *
 * An example - "newyork city" - > "new york city"
 *              "christmasday" - > "christmas day"
 *
 * Like other Operators it returns an ITuple which contains a list of strings
 * which are the modified queries.
 *
 */
public class QueryRewriter implements IOperator{

    private String searchQuery;
    private FuzzyTokenizer fuzzyTokenizer;

    public static final String QUERYLIST = "querylist";
    public static final Attribute QUERYLIST_ATTR = new Attribute(QUERYLIST, FieldType.LIST);
    public static final Schema SCHEMA_QUERY_LIST = new Schema(QUERYLIST_ATTR);

    private ITuple itupleResult;

    /**
     * Parameterized constructor that requires a Search Query String that
     * is to be rewritten
     * @param searchQuery
     */
    public QueryRewriter(String searchQuery) {
        this.searchQuery = searchQuery;
    }

    /**
     * Overridden Open method of the IOperator interface
     * Query parser object will be created here
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        this.fuzzyTokenizer = new FuzzyTokenizer(searchQuery);
        this.itupleResult = null;
    }

    /**
     * Calls appropriate implementation methods to populate the list
     * of rewritten search queries, and constructing a tuple from it. 
     * @return - Tuple with the rewritten queries as a comma separated string
     * @throws Exception
     */
    @Override
    public ITuple getNextTuple() throws Exception {

        boolean NOT_OPEN = (fuzzyTokenizer == null);    //Ensures QueryRewriter is open before calling getNextTuple
        boolean EOF = (itupleResult != null);   //Ensures you can call QueryRewriter.getNextTuple only once

        if(NOT_OPEN || EOF)
            return null;
        else {
            List<String> queryStrings = fuzzyTokenizer.getFuzzyTokens();
            IField[] iFieldResult = {new ListField(queryStrings)};
            itupleResult = new DataTuple(SCHEMA_QUERY_LIST, iFieldResult);
            return itupleResult;
        }
    }

    /**
     * Closing the query Parser object
     * @throws Exception
     */
    //TODO - take care of the case where close is called before open
    @Override
    public void close() throws Exception {
        this.fuzzyTokenizer = null;
        this.searchQuery = null;
        this.itupleResult = null;
    }
}