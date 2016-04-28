package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 * QueryRewriter is an operator that converts a query search string which
 * have faults in space placement to a list of meaningful search queries
 * QueryRewriter internally uses the FuzzyTokenizer class to fulfill
 * its functionality.
 *
 * Like other Operators it returns an ITuple which contains a list of strings
 * which are the modified queries.
 *
 */
public class QueryRewriter implements IOperator{

    private String searchQuery;
    //Uncomment below line when FuzzyTokenizer has been created
    //private FuzzyTokenizer fuzzyTokenizer;

    public static final String QUERYLIST = "querylist";
    public static final Attribute QUERYLIST_ATTR = new Attribute(QUERYLIST, FieldType.LIST);
    public static final List<Attribute> ATTRIBUTES_QUERY_LIST = Arrays.asList(QUERYLIST_ATTR);
    public static final Schema SCHEMA_QUERY_LIST = new Schema(ATTRIBUTES_QUERY_LIST);

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
     * Overriden Open method of the IOperator interface
     * Query parser object will be created here
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        //Uncomment below line when FuzzyTokenizer has been created
        //fuzzyTokenizer = new FuzzyTokenizer(searchQuery);

    }

    /**
     * Calls appropriate implementation methods to populate the list
     * of rewritten search queries, and constructing a tuple from it. 
     * @return - Tuple with the rewritten queries as a comma separated string
     * @throws Exception
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        //Uncomment below line when FuzzyTokenizer has been created
        //List<String> queryStrings = Arrays.asList(fuzzyTokenizer.getParsedQueries());

        List<String> queryStrings = Arrays.asList(searchQuery);
        IField[] iFieldResult = {new ListField(queryStrings)};
        itupleResult = new DataTuple(SCHEMA_QUERY_LIST, iFieldResult);
        return itupleResult;
    }

    /**
     * Closing the query Parser object
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        //Uncomment below line when FuzzyTokenizer has been created
        //fuzzyTokenizer = null;
    }
}
