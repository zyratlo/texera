package edu.uci.ics.textdb.dataflow.keywordmatch;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.*;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexSearchSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author Prakul
 *
 */


public class KeywordMatcherTest {

    private KeywordMatcher keywordMatcher;
    private IDataWriter dataWriter;
    private IDataReader dataReader;
    private LuceneDataStore dataStore;
    private IndexSearchSourceOperator indexSearchSourceOperator;

    private Analyzer analyzer;
    private Query queryObj;
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        analyzer = new StandardAnalyzer();
        dataWriter = new LuceneDataWriter(dataStore, analyzer);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
        schema = dataStore.getSchema();
    }

    @After
    public void cleanUp() throws Exception {
        dataWriter.clearData();
    }

    /**
     * Tokenizes the query string using the given analyser
     * @param analyzer
     * @param query
     * @return
     */
    public ArrayList<String> queryTokenizer(Analyzer analyzer,  String query) {
        HashSet<String> resultSet = new HashSet<>();
        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream  = analyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        try{
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String term = charTermAttribute.toString();
                resultSet.add(term);
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        result.addAll(resultSet);

        return result;
    }

    /**
     * Creates a Query object as a boolean Query on all attributes
     * @param query
     * @param attributeList
     * @return
     * @throws ParseException
     */
    private Query createQueryObject(String query, ArrayList<Attribute> attributeList) throws ParseException {
        Analyzer analyzer = new StandardAnalyzer();
        ArrayList<String> tokens;
        String[] fields = new String[attributeList.size()];
        for (int i=0;i< attributeList.size();i++){
            fields[i] = attributeList.get(i).getFieldName();
        }

        tokens = queryTokenizer(analyzer, query);
        BooleanQuery booleanQuery = new BooleanQuery();
        MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer);
        for(String searchToken: tokens){
            Query termQuery = parser.parse(searchToken);
            booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
        }
        return booleanQuery;
    }


    /**
     * For a given string query & list of attributes it gets a list of results
     * buildMultiQueryOnAttributeList flag decides if the query is formed as a boolean Query on all attribute
     * or all records are scanned
     * @param query
     * @param attributeList
     * @param buildMultiQueryOnAttributeList
     * @return
     * @throws DataFlowException
     * @throws ParseException
     */

    public List<ITuple> getQueryResults(String query, ArrayList<Attribute> attributeList, boolean buildMultiQueryOnAttributeList) throws DataFlowException, ParseException {

        Analyzer analyzer = new StandardAnalyzer();
        IPredicate predicate = new KeywordPredicate(query, attributeList, analyzer);
        QueryParser queryParser;
        if(!buildMultiQueryOnAttributeList) {
            queryParser = new QueryParser(TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), analyzer);
            queryObj = queryParser.parse(LuceneConstants.SCAN_QUERY);
        }
        else {
            queryObj = createQueryObject(query, attributeList);
        }
        IDataReader dataReader = new LuceneDataReader(dataStore, queryObj);
        indexSearchSourceOperator = new IndexSearchSourceOperator(dataReader);
        keywordMatcher = new KeywordMatcher(predicate, indexSearchSourceOperator);
        keywordMatcher.open();
        List<ITuple> results = new ArrayList<ITuple>();
        ITuple nextTuple = null;
        while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        indexSearchSourceOperator.close();
        return results;
    }

    /**
     * For a given string query it gets a list of results
     *
     * @param query
     * @return List<ITuple>
     * @throws DataFlowException
     * @throws ParseException
     */
    public List<ITuple> getQueryResults(String query) throws DataFlowException, ParseException {
        String defaultField = TestConstants.ATTRIBUTES_PEOPLE[5].getFieldName();

        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.ATTRIBUTES_PEOPLE[5]);
        Analyzer analyzer = new StandardAnalyzer();
        IPredicate predicate = new KeywordPredicate(query, attributeList, analyzer);
        QueryParser queryParser = new QueryParser(defaultField, analyzer);
        queryObj = queryParser.parse(query);
        IDataReader dataReader = new LuceneDataReader(dataStore, queryObj);
        indexSearchSourceOperator = new IndexSearchSourceOperator(dataReader);
        keywordMatcher = new KeywordMatcher(predicate, indexSearchSourceOperator);
        keywordMatcher.open();

        List<ITuple> results = new ArrayList<ITuple>();
        ITuple nextTuple = null;
        while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        indexSearchSourceOperator.close();
        return results;
    }

    /**
     * Verifies Keyword Matcher on multiword string
     * @throws Exception
     */
    @Test
    public void testKeywordMatcher() throws Exception {
        String query = "short tall";
        List<ITuple> results = getQueryResults(query);
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        for(ITuple t : results){
            boolean contains = TestUtils.contains(tuples, t, Arrays.asList(TestConstants.ATTRIBUTES_PEOPLE));
            Assert.assertTrue(contains);
        }
        Assert.assertEquals(4,results.size());
    }

    /**
     * Verifies GetNextTuple of Keyword Matcher and single
     * word queries in String Field
     * @throws Exception
     */
    @Test
    public void testSingleWordQueryInStringField() throws Exception {
        //Prepare Query
        String query = "Bruce";
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 5, "Bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<ITuple>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getQueryResults(query, attributeList, true);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
        Assert.assertEquals(1,resultList.size());
    }


    /**
     * Verifies GetNextTuple of Keyword Matcher and single
     * word queries in Text Field
     * @throws Exception
     */

    @Test
    public void testSingleWordQueryInTextField() throws Exception {
        //Prepare Query
        String query = "tall";
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "Tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                new TextField("Tall Angry"), new ListField<Span>(list) };

        IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
                new IntegerField(42), new DoubleField(5.99),
                new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"),
                new ListField<Span>(list) };
        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);

        List<ITuple> expectedResultList = new ArrayList<ITuple>();
        expectedResultList.add(tuple1);
        expectedResultList.add(tuple2);

        //Perform Query
        List<ITuple> resultList = getQueryResults(query, attributeList, true);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
        Assert.assertEquals(2, resultList.size());
    }


    /**
     * Verifies List<ITuple> returned by Keyword Matcher on multiple
     * word queries
     * @throws Exception
     */
    @Test
    public void testMultipleWordsQuery() throws Exception {
        //Prepare Query
        String query = "george lin lin";
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 0, 14, "george lin lin", "george lin lin");
        Span span2 = new Span("description", 0, 3, "lin", "Lin");
        Span span3 = new Span("description", 25, 28, "lin", "lin");
        list.add(span1);
        list.add(span2);
        list.add(span3);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<ITuple>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getQueryResults(query, attributeList, false);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
        Assert.assertEquals(1,resultList.size());
    }


    /**
     * Verifies: data source has multiple attributes, and an entity
     * can appear in all the fields and multiple times.
     * @throws Exception
     */
    @Test
    public void testWordInMultipleFieldsQuery() throws Exception {
        //Prepare Query
        String query = "lin clooney";
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(TestConstants.FIRST_NAME_ATTR);
        attributeList.add(TestConstants.LAST_NAME_ATTR);
        attributeList.add(TestConstants.DESCRIPTION_ATTR);

        //Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("lastName", 0, 11, "lin clooney", "lin clooney");
        Span span2 = new Span("description", 0, 3, "lin", "Lin");
        Span span3 = new Span("description", 25, 28, "lin", "lin");
        Span span4 = new Span("description", 4, 11, "clooney", "Clooney");
        Span span5 = new Span("description", 29, 36, "clooney", "clooney");
        list.add(span1);
        list.add(span2);
        list.add(span3);
        list.add(span4);
        list.add(span5);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<Span>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<ITuple>();
        expectedResultList.add(tuple1);

        //Perform Query
        List<ITuple> resultList = getQueryResults(query, attributeList, false);

        //Perform Check
        boolean contains = TestUtils.containsAllResults(expectedResultList, resultList);
        Assert.assertTrue(contains);
        Assert.assertEquals(1,resultList.size());
    }




}
