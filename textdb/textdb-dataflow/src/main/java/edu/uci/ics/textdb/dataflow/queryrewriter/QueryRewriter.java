package edu.uci.ics.textdb.dataflow.queryrewriter;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringListField;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kishorenarendran on 25/04/16.
 */
public class QueryRewriter implements IOperator{

    private String searchQuery;
    private FuzzyTokenizer fuzzyTokenizer;

    public static final String QUERYLIST = "querylist";
    public static final Attribute QUERYLIST_ATTR = new Attribute(QUERYLIST, FieldType.STRING_LIST);
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
        fuzzyTokenizer = new FuzzyTokenizer(searchQuery);

    }

    /**
     * Calling appropriate fuzzyTokenizer method to populate the list
     * of rewritten search queries, and constructing a tuple from it. 
     * @return - Tuple with the rewritten queries as a comma separated string
     * @throws Exception
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        List<String> queryStrings = Arrays.asList(fuzzyTokenizer.getParsedQueries());
        IField[] iFieldResult = {new StringListField(queryStrings)};
        itupleResult = new DataTuple(SCHEMA_QUERY_LIST, iFieldResult);
        return itupleResult;
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
