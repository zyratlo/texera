package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.relation.RelationManager;
import junit.framework.Assert;

/**
 * JoinDistanceTest tests the Join Operator and the JoinDistancePredicate.
 * 
 * The test classes are organized as the following:
 *   JoinDistanceTest   includes (only) the main test logic.
 *   JoinDistanceHelper includes helper functions and wrapper functions.
 *   JoinTestConstants  includes the schema and data that Join tests use.
 * 
 * @author sripadks
 * @author Zuozhi Wang
 *
 */
public class JoinDistanceTest {
    
    public static final String BOOK_TABLE_OUTER = JoinDistanceHelper.BOOK_TABLE_OUTER;
    public static final String BOOK_TABLE_INNER = JoinDistanceHelper.BOOK_TABLE_INNER;
    
    public static final KeywordMatchingType conjunction = KeywordMatchingType.CONJUNCTION_INDEXBASED;
    public static final KeywordMatchingType phrase = KeywordMatchingType.PHRASE_INDEXBASED;

    
    /*
     * The annotators @BeforeClass and @AfterClass are used instead of @Before and @After.
     * 
     * The difference is that:
     *   @Before and @After are executed before and after EACH test case.
     *   @BeforeClass and @AfterClass are executed once before ALL the test begin and ALL the test have finished.
     * 
     * We don't want to create and delete the tables on every test case, 
     *   therefore BeforeClass and AfterClass are better options.
     *   
     */
    @BeforeClass
    public static void setup() throws TextDBException {
        // writes the test tables before ALL tests
        JoinDistanceHelper.createTestTables();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        // deletes the test tables after ALL tests
        JoinDistanceHelper.deleteTestTables();
    }
    
    /*
     * The @After annotation is used here because we want to clear the data in two tables after EVERY test case. 
     * This is because different test cases write different data in to the test tables, 
     *   and they need to be cleared before the next test case begins.
     */
    @After
    public void clearTables() throws TextDBException {
        // clear the test tables after EACH test
        JoinDistanceHelper.clearTestTables();
    }
    
    /*
     * This case tests for scenario when the IDs of the documents don't match.
     * Test result: The list of results returned is empty.
     */
    @Test
    public void testIdsDontMatch() throws Exception {        
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1);
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup2);
        
        String query = "special";
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, query, conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, query, conjunction);

        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 10, Integer.MAX_VALUE, 0);
        
        Assert.assertEquals(0, resultList.size());
    }
    

    /*
     * This case tests for the scenario when the IDs of the documents match,
     * fields to join match and the difference of keyword spans
     *  is withinthe given span threshold.
     *  e.g.
     *  [<11, 18>]
     *  [<27, 33>]
     *  threshold = 20
     *  [           ]
     *          [          ]
     *  <-------->
     *           <-------> (within threshold)
     * Test result: The list contains a tuple with all the fields and a span
     * list consisting of the joined span. The joined span is made up of the
     * field name, start and stop index (computed as <min(span1 spanStartIndex,
     * span2 spanStartIndex), max(span1 spanEndIndex, span2 spanEndIndex)>)
     * key (combination of span1 key and span2 key) and value (combination of
     * span1 value and span2 value).
     * [<11, 33>]
     */
    @Test
    public void testIdsMatchFieldsMatchSpanWithinThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        
        ITuple expectedTuple = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // This case tests for the scenario when the IDs match, fields to join match
    // but the difference of keyword spans to be joined is greater than the
    // threshold.
    // e.g.
    // [<11, 18>]
    // [<42, 48>]
    // threshold = 20
    // [           ]
    //           [     ]
    // <--------> (beyond threshold)
    // Test result: An empty list is returned.
    @Test
    public void testIdsMatchFieldsMatchSpanExceedThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "topics", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);

        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when either/both of the operators'
    // result lists are empty (i.e. when one/both of the operators' are
    // not able to find any suitable matches)
    // Test result: Join should return an empty list.
    @Test
    public void testOneOfTheOperatorResultIsEmpty() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the operators result lists has no span. 
    // If one of the operators doesn't have span, then an exception will be thrown.
    // Test result: DataFlowException is thrown
    @Test(expected = DataFlowException.class)
    public void testOneOfTheOperatorResultContainsNoSpan() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);

        String fuzzyTokenQuery = "this writer writes well";
        double thresholdRatio = 0.25;
        List<String> textAttributeNames = JoinTestConstants.BOOK_SCHEMA.getAttributes().stream()
                .filter(attr -> attr.getFieldType() != FieldType.TEXT)
                .map(attr -> attr.getFieldName()).collect(Collectors.toList());
        FuzzyTokenPredicate fuzzyPredicateInner = new FuzzyTokenPredicate(fuzzyTokenQuery, textAttributeNames,
                LuceneAnalyzerConstants.getStandardAnalyzer(), thresholdRatio);
        FuzzyTokenMatcher fuzzyMatcherInner = new FuzzyTokenMatcher(fuzzyPredicateInner);
        
        DataStore innerDataStore = RelationManager.getRelationManager().getTableDataStore(BOOK_TABLE_INNER);
        fuzzyMatcherInner.setInputOperator(
                new IndexBasedSourceOperator(fuzzyPredicateInner.getDataReaderPredicate(innerDataStore)));

        ProjectionPredicate removeSpanListPredicate = new ProjectionPredicate(
                JoinTestConstants.BOOK_SCHEMA.getAttributeNames());
        ProjectionOperator removeSpanListProjection = new ProjectionOperator(removeSpanListPredicate);
        removeSpanListProjection.setInputOperator(fuzzyMatcherInner);
        
        JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, removeSpanListProjection, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);       
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the spans to be joined encompasses the other span
    // and both |(span 1 spanStartIndex) - (span 2 spanStartIndex)|,
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| are within threshold.
    // e.g.
    // [<11, 18>]
    // [<3, 33>]
    // threshold = 20
    // [              ]
    //      [   ]
    // <---->   <-----> (within threshold)
    // Test result: The bigger span should be returned.
    // [<3, 33>]
    @Test
    public void testOneSpanEncompassesOtherAndDifferenceLessThanThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "takes a special kind of writer", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        
        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 3, 33, "special_takes a special " + "kind of writer",
                "takes a special " + "kind of writer");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        
        ITuple expectedTuple = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but one of the spans to be joined encompasses the other span
    // and |(span 1 spanStartIndex) - (span 2 spanStartIndex)|
    // and/or |(span 1 spanEndIndex) - (span 2 spanEndIndex)| exceed threshold.
    // e.g.
    // [<11, 18>]
    // [<3, 33>]
    // threshold = 20
    // [             ]
    //     [    ]
    // <-->     <---> (beyond threshold)
    // Test result: Join should return an empty list.
    @Test
    public void testOneSpanEncompassesOtherAndDifferenceGreaterThanThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "takes a special kind of writer", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 10, Integer.MAX_VALUE, 0);
        
        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined have some overlap and both
    // |(span 1 spanStartIndex) - (span 2 spanStartIndex)|,
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| are within threshold.
    // e.g.
    // [<75, 97>]
    // [<92, 109>]
    // threshold = 20
    // [      ]
    //      [         ]
    // <----> <-------> (within threshold)
    // Test result: The list contains a tuple with all the fields and a span
    // list consisting of the joined span. The joined span is made up of the
    // field name, start and stop index (computed as <min(span1 spanStartIndex,
    // span2 spanStartIndex), max(span1 spanEndIndex, span2 spanEndIndex)>)
    // key (combination of span1 key and span2 key) and value (combination of
    // span1 value and span2 value).
    // [<75, 109>]
    @Test
    public void testSpansOverlapAndWithinThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "gastrointestinal tract", phrase);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "tract interesting", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 75, 109, "gastrointestinal tract_" + "tract interesting",
                "gastrointestinal " + "tract interesting");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined have some overlap and
    // |(span 1 spanStartIndex) - (span 2 spanStartIndex)| and/or
    // |(span 1 spanEndIndex) - (span 2 spanEndIndex)| exceed threshold.
    // e.g.
    // [<75, 97>]
    // [<92, 109>]
    // threshold = 10
    // [    ]
    //    [        ]
    // <--> <-----> (beyond threshold)
    // Test result: Join should return an empty list.
    @Test
    public void testSpansOverlapAndExceedThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "takes a special", phrase);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "special kind of writer", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 10, Integer.MAX_VALUE, 0);

        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when the IDs match, fields to be joined
    // match, but the spans to be joined are the same, i.e. both the keywords
    // are same.
    // e.g.
    // [<11, 18>]
    // [<11, 18>]
    // threshold = 20 (can be any non-negative number)
    // [ ]
    // [ ]
    // Test result: Join should return same span and key and the value in span
    // should be the same.
    // [<11, 18>]
    @Test
    public void testBothTheSpansAreSame() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "special", conjunction);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 18, "special_special", "special");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }

    // This case tests for the scenario when the specified ID field of either/
    // both of the operators' does not exist.
    // In this case, an exception is thrown to warn user that join can't be performed without ID field.
    // Test result: DataFlowException is thrown
    @Test(expected = DataFlowException.class)
    public void testIDFieldDoesNotExist() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        ProjectionPredicate projectionPredicate = new ProjectionPredicate(
                Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA).getAttributeNames().stream()
                .filter(attrName -> ! attrName.equals(JoinTestConstants.ID)).collect(Collectors.toList()));
        ProjectionOperator removeIDProject = new ProjectionOperator(projectionPredicate);
        removeIDProject.setInputOperator(keywordSourceInner);
        
        JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, removeIDProject, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
    }
    
    // -----------------<Test cases for intersection of tuples>----------------

    // This case tests for the scenario when the tuples have different sets of
    // attributes (hence different schemas) barring the attribute join has to
    // be performed upon (for this case, threshold condition is satisfied).
    // e.g. Schema1: {ID, Author, Pages, Review}
    //      Schema2: {ID, Title, Pages, Review}
    //      Join Attribute: Review
    // Test result: Join should result in a list with a single tuple which has
    // the attributes common to both the tuples and the joined span.
    @Test
    public void testAttributesAndFieldsIntersection() throws TextDBException {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(0));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(0));
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        ProjectionPredicate projectionPredicateOuter = new ProjectionPredicate(
                Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA).getAttributeNames().stream()
                .filter(attrName -> ! attrName.equals(JoinTestConstants.TITLE)).collect(Collectors.toList()));
        ProjectionOperator projectionOuter = new ProjectionOperator(projectionPredicateOuter);
        projectionOuter.setInputOperator(keywordSourceOuter);
        
        ProjectionPredicate projectionPredicateInner = new ProjectionPredicate(
                Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA).getAttributeNames().stream()
                .filter(attrName -> ! attrName.equals(JoinTestConstants.AUTHOR)).collect(Collectors.toList()));
        ProjectionOperator projectionInner = new ProjectionOperator(projectionPredicateInner);
        projectionInner.setInputOperator(keywordSourceInner);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(projectionOuter, projectionInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = new Schema(JoinTestConstants.ID_ATTR, JoinTestConstants.PAGES_ATTR, 
                JoinTestConstants.REVIEW_ATTR, SchemaConstants.SPAN_LIST_ATTRIBUTE);

        List<Span> spanList = new ArrayList<>();
        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(resultSchema, book);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    /*
     * This case tests for the scenario when one of the attributes (not the
     * one join is performed upon) has different field values (assume threshold
     * condition is satisfied).
     * 
     * e.g. Schema1: {ID, Author, Title, Pages, Review}
     *      Values1: { 2,      A,     B,     5,    ABC}
     *      Schema2: {ID, Author, Title, Pages, Review}
     *      Values2: { 2,      A,     C,     5,    ABC}
     * 
     * The values of "Title" field are different, since "title" is not join attribute,
     * JoinOperator remains silent about the difference, and use the innerTuple's value.
     * 
     * Test result: the output value of Title Field is value of innerTuple, in this test case, "Grunt".
     */
    @Test
    public void testWhenAttributeFieldsAreDifferent() throws Exception {
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(0);
        ITuple alteredTuple = JoinDistanceHelper.alterField(originalTuple, 2, new StringField("C"));
        
        JoinDistanceHelper.insertToOuter(originalTuple);
        JoinDistanceHelper.insertToInner(alteredTuple);
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = Utils.addAttributeToSchema(JoinTestConstants.BOOK_SCHEMA, SchemaConstants.SPAN_LIST_ATTRIBUTE);
        
        List<Span> spanList = new ArrayList<>();
        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new StringField("Mary Roach"), new StringField("C"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(resultSchema, book);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // This case tests for the scenario when one of the attributes (the one
    // join is performed upon) has different field values (assume threshold
    // condition is satisfied).
    // e.g. Schema1: {ID, Author, Title, Pages, Review}
    //      Values1: { 2,      A,     B,     5,    ABC}
    //      Schema2: {ID, Author, Title, Pages, Review}
    //      Values2: { 2,      A,     B,     5,     AB}
    //      Join Attribute: Review
    // Test result: An empty list is returned.
    @Test
    public void testJoinAttributeFieldsAreDifferent() throws Exception {
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(0);
        
        // the sentence in altered tuple is incomplete, while the keyword "writer" is still in there
        ITuple alteredTuple = JoinDistanceHelper.alterField(originalTuple, 4, 
                new TextField("It takes a special kind of writer to make topics ranging from death to our "));
        
        JoinDistanceHelper.insertToOuter(originalTuple);
        JoinDistanceHelper.insertToInner(alteredTuple);
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);

        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when an attribute (not the one join is
    // performed upon) has same name and same respective field value but has
    // different field types.
    // Test result: The attribute and the respective field in question is
    // dropped from the result tuple.
    @Test
    public void testWhenAttributeOfSameNameAreDifferent() throws Exception {
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(0);
        JoinDistanceHelper.insertToOuter(originalTuple);

        // create a table for the altered tuple (since the schema is different)
        String BOOK_TABLE_SPECIAL = "join_test_book_special";
        
        Schema alteredSchema = new Schema(JoinTestConstants.BOOK_SCHEMA.getAttributes().stream()
                .map(attr -> attr.getFieldName() != JoinTestConstants.AUTHOR ? attr : 
                        new Attribute(JoinTestConstants.AUTHOR, FieldType.TEXT)).toArray(Attribute[]::new));
        
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(BOOK_TABLE_SPECIAL);
        relationManager.createTable(BOOK_TABLE_SPECIAL, "../index/test_tables/" + BOOK_TABLE_SPECIAL, 
                alteredSchema, LuceneAnalyzerConstants.standardAnalyzerString());
              
        // the type of the "author" field is changed from STRING to TEXT
        // alterField function will take care of the changes of the tuple's schema
        ITuple alteredTuple = JoinDistanceHelper.alterField(originalTuple, 1, 
                new TextField(originalTuple.getField(1).getValue().toString()));
        
        relationManager.insertTuple(BOOK_TABLE_SPECIAL, alteredTuple);
                        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_SPECIAL, "writer", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = new Schema(JoinTestConstants.ID_ATTR, JoinTestConstants.TITLE_ATTR,
                JoinTestConstants.PAGES_ATTR, JoinTestConstants.REVIEW_ATTR, SchemaConstants.SPAN_LIST_ATTRIBUTE);

        List<Span> spanList = new ArrayList<>();
        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book = { new IntegerField(52), new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(resultSchema, book);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
        
        relationManager.deleteTable(BOOK_TABLE_SPECIAL);
    }

    /*
     * This case tests for the scenario when an attribute (the one join is
     * performed upon) has same name, but different field types.
     * 
     * Attributes must have same name and type to be considered equal.
     * 
     * In this case, since it's the attribute to be joined is not the same,
     * an exception is thrown to warn user that join cannot be performed.
     * 
     * Test result: DataFlowException is thrown.
     * 
     */
    @Test(expected = DataFlowException.class)
    public void testJoinAttributeOfSameNameHaveDifferentFieldType() throws Exception {
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(0);
        JoinDistanceHelper.insertToOuter(originalTuple);

        // create a table for the altered tuple (since the schema is different)
        String BOOK_TABLE_SPECIAL = "join_test_book_special";
        
        Schema alteredSchema = new Schema(JoinTestConstants.BOOK_SCHEMA.getAttributes().stream()
                .map(attr -> attr.getFieldName() != JoinTestConstants.REVIEW ? attr : 
                        new Attribute(JoinTestConstants.REVIEW, FieldType.STRING)).toArray(Attribute[]::new));
        
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(BOOK_TABLE_SPECIAL);
        relationManager.createTable(BOOK_TABLE_SPECIAL, "../index/test_tables/" + BOOK_TABLE_SPECIAL, 
                alteredSchema, LuceneAnalyzerConstants.standardAnalyzerString());
              
        // the type of the "author" field is changed from STRING to TEXT
        // alterField function will take care of the changes of the tuple's schema
        ITuple alteredTuple = JoinDistanceHelper.alterField(originalTuple, 4, 
                new StringField(originalTuple.getField(4).getValue().toString()));
        
        relationManager.insertTuple(BOOK_TABLE_SPECIAL, alteredTuple);
                        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_SPECIAL, "writer", conjunction);
        
        JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        relationManager.deleteTable(BOOK_TABLE_SPECIAL);
    }
    
    // --------------------<END of single tuple test cases>--------------------

    // This case tests for the scenario when both the operators' have multiple
    // tuples and none of the tuples have same ID (multi-tuple version of the
    // case when IDs don't match).
    // Test result: Join should result in an empty list.
    @Test
    public void testMultiTupleIdsDontMatch() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1;
        List<ITuple> innerTuples = JoinTestConstants.bookGroup2;
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "review", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);

        Assert.assertEquals(0, resultList.size());
    }

    // This case tests for the scenario when one of the operators' has multiple
    // tuples and the other has a single tuple (ID of one of the tuple's in the
    // list of multiple tuples should match with the ID of the single tuple) and
    // spans are within the threshold.
    // e.g.
    // ID:          1         2         3         4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID:         2
    // Tuple: [<62, 66>]
    // threshold = 12
    // [      ] [ ] [ ] [ ]
    //       [      ]
    // <----->
    //        <-----> (ID match, within threshold)
    // Test result: Join should result in a list with a single tuple with the
    // matched ID and the corresponding joined spans.
    // Tuple: [<62, 73>]
    @Test
    public void testMultipleTuplesAndSingleTupleSpanWithinThreshold() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1;
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(4, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "review", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 0, 16, "review_book", "Review of a " + "Book");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 62, 73, "review_book", "book review");
        spanList.add(span2);
        Span span3 = new Span(JoinTestConstants.REVIEW, 235, 246, "review_book", "book review");
        spanList.add(span3);

        IField[] book1 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };
        
        ITuple expectedTuple = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // This case tests for the scenario when one of the operators' has multiple
    // tuples and the other has a single tuple (ID of one of the tuple's in the
    // list of multiple tuples should match with the ID of the single tuple) and
    // none of the spans are not within threshold.
    // e.g.
    // ID:          1         2         3         4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID:         2
    // Tuple: [<62, 66>]
    // threshold = 4
    // [ ] [ ] [ ] [ ]
    //      [ ]
    // <--->
    // <--> (ID match, beyond threshold)
    // Test result: Join should result in an empty list.
    @Test
    public void testMultipleTuplesAndSingleTupleSpanExceedThreshold() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1;
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(4, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "review", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 4, Integer.MAX_VALUE, 0);

        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when both the operators' have multiple
    // tuples and some of tuples IDs match and spans are within threshold.
    // e.g.
    // ID:          1         2         3         4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID:          2          4
    // Tuples: [<62, 66>] [<62, 66>]
    // threshold = 12
    // [       ]            [      ] [ ] [ ]
    //       [      ] [       ] [ ] [ ] [ ]
    // <-----> <---->
    //                <-----> <---->(ID match, within threshold)
    // Test result: Join should result in a list containing tuples with spans.
    // The number of tuples is equal to the number of tuples with both ID match
    // and span within threshold.
    // [<62, 73>][<62, 73>]
    @Test
    public void testBothOperatorsMultipleTuplesSpanWithinThreshold() throws Exception {
        List<ITuple> outerTuples = new ArrayList<>();
        outerTuples.add(JoinTestConstants.bookGroup1.get(3));
        outerTuples.add(JoinTestConstants.bookGroup2.get(2));
        outerTuples.add(JoinTestConstants.bookGroup2.get(4));
        
        List<ITuple> innerTuples = new ArrayList<>();
        innerTuples.addAll(JoinTestConstants.bookGroup1);
        innerTuples.addAll(JoinTestConstants.bookGroup2);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "review", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 12, Integer.MAX_VALUE, 0);

        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 0, 16, "review_book", "Review of a " + "Book");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 62, 73, "review_book", "book review");
        spanList.add(span2);
        Span span3 = new Span(JoinTestConstants.REVIEW, 235, 246, "review_book", "book review");
        spanList.add(span3);

        IField[] book1 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(65), new StringField("Sharon Guskin"),
                new StringField("The Forgetting Time: A Novel"), new IntegerField(368),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(63), new StringField("Paul Kalanithi"),
                new StringField("When Breath Becomes Air"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(resultSchema, book1);
        ITuple expectedTuple2 = new DataTuple(resultSchema, book2);
        ITuple expectedTuple3 = new DataTuple(resultSchema, book3);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        
        Assert.assertEquals(3, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    
    
    // This case tests for the scenario when both the operators' have multiple
    // tuples and some of tuples IDs match, but none of spans are within
    // threshold.
    // e.g.
    // ID:          1         2         3         4
    // Tuples: [<67, 73>][<67, 73>][<67, 73>][<67, 73>]
    // ID:          2          4
    // Tuples: [<62, 66>] [<62, 66>]
    // threshold = 4
    // [     ]        [      ]       [      ] [ ]
    // [ ] [ ] [       ]      [       ] [ ]
    //         <-----> <---->
    //                        <-----> <---->(ID match, beyond threshold)
    // Test result: Join should result in an empty list.
    @Test
    public void testBothOperatorsMultipleTuplesSpanExceedThreshold() throws Exception {
        List<ITuple> outerTuples = new ArrayList<>();
        outerTuples.add(JoinTestConstants.bookGroup1.get(3));
        outerTuples.add(JoinTestConstants.bookGroup2.get(2));
        outerTuples.add(JoinTestConstants.bookGroup2.get(4));
        
        List<ITuple> innerTuples = new ArrayList<>();
        innerTuples.addAll(JoinTestConstants.bookGroup1);
        innerTuples.addAll(JoinTestConstants.bookGroup2);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "review", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "book", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 4, Integer.MAX_VALUE, 0);
        Assert.assertEquals(0, resultList.size());
    }
    
    // This case tests for the scenario when the query has results over multiple
    // fields and join has to be performed only on the field mentioned in the
    // attribute.
    // Test result: Join should return only those tuples which satisfy all the
    // constraints.
    @Test
    public void testQueryHasResultsOverMultipleFields() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, Integer.MAX_VALUE, 0);

        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);
        
        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(53), new StringField("Noah Hawley"), new StringField("Before the Fall"),
                new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book4 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(resultSchema, book1);
        ITuple expectedTuple2 = new DataTuple(resultSchema, book2);
        ITuple expectedTuple3 = new DataTuple(resultSchema, book3);
        ITuple expectedTuple4 = new DataTuple(resultSchema, book4);
        List<ITuple> expectedResult = new ArrayList<>();
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);
        
        Assert.assertEquals(4, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }
    
    // ---------------------<Limit and offset test cases.>---------------------

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and less than the actual number of results and offset is 0 and join 
     * is performed.
     * Test result: A list of tuples with number of tuples equal to limit.
     */
    @Test
    public void testForLimitWhenLimitIsLesserThanActualNumberOfResults() throws Exception{
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 3, 0);

        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();
        
        Span span1 = new Span(JoinTestConstants.REVIEW, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };
        
        IField[] book2 = { new IntegerField(53), new StringField("Noah Hawley"),
                new StringField("Before the Fall"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };
        
        IField[] book4 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(resultSchema, book1);
        ITuple expectedTuple2 = new DataTuple(resultSchema, book2);
        ITuple expectedTuple3 = new DataTuple(resultSchema, book3);
        ITuple expectedTuple4 = new DataTuple(resultSchema, book4);
        List<ITuple> expectedResult = new ArrayList<>(3);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);
        
        Assert.assertEquals(3, resultList.size());
        Assert.assertTrue(TestUtils.containsAll(expectedResult, resultList));
    }

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and greater than the actual number of results and offset is 0 and join
     * is performed.
     * Test result: A list of tuples with number of tuples equal to the maximum
     * number of tuples operator can generate (which is lesser than limit.)
     */
    @Test
    public void testForLimitWhenLimitIsGreaterThanActualNumberOfResults() throws Exception{
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 10, 0);

        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(51), new StringField("author unknown"), new StringField("typical"),
                new IntegerField(300),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book2 = { new IntegerField(53), new StringField("Noah Hawley"), new StringField("Before the Fall"),
                new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book3 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        IField[] book4 = { new IntegerField(55), new StringField("Matti Friedman"),
                new StringField("Pumpkinflowers: A Soldier's " + "Story"), new IntegerField(256),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(resultSchema, book1);
        ITuple expectedTuple2 = new DataTuple(resultSchema, book2);
        ITuple expectedTuple3 = new DataTuple(resultSchema, book3);
        ITuple expectedTuple4 = new DataTuple(resultSchema, book4);
        List<ITuple> expectedResult = new ArrayList<>(5);
        expectedResult.add(expectedTuple1);
        expectedResult.add(expectedTuple2);
        expectedResult.add(expectedTuple3);
        expectedResult.add(expectedTuple4);

        Assert.assertEquals(4, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }

    /*
     * This case tests for the scenario when limit is 0 and offset is 0 and 
     * join is performed.
     * Test result: An empty list.
     */
    @Test
    public void testForLimitWhenLimitIsZero() throws Exception{
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 0, 0);

        Assert.assertEquals(0, resultList.size());
    }

    /*
     * This case tests for the scenario when limit is 0 and offset is some 
     * integer greater than 0 and less than the actual number of results and 
     * join is performed.
     * Test result: An empty list.
     */
    @Test
    public void testForLimitWhenLimitIsZeroAndHasOffset() throws Exception{
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 0, 2);

        Assert.assertEquals(0, resultList.size());
    }

    /*
     * This case tests for the scenario when limit is some integer greater than
     * 0 and less than the actual number of results and offset is some integer
     * greater than 0 and less than actual number of results and join is 
     * performed.
     * Test result: A list of tuples with number of tuples equal to limit 
     * starting from the set offset.
     */
    @Test
    public void testForLimitWhenLimitIsLesserThanActualNumberOfResultsAndHasOffset() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 1, 2);

        Schema resultSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 28, 119, "typical_actually", "typical review. "
                + "This is a test. A book review test. " + "A test to test queries without actually");
        spanList.add(span1);
        Span span2 = new Span(JoinTestConstants.REVIEW, 186, 234, "typical_actually",
                "actually a review " + "even if it is not your typical");
        spanList.add(span2);

        IField[] book1 = { new IntegerField(54), new StringField("Andria Williams"),
                new StringField("The Longest Night: A Novel"), new IntegerField(400),
                new TextField("Review of a Book. This is a typical " + "review. This is a test. A book review "
                        + "test. A test to test queries without " + "actually using actual review. From "
                        + "here onwards, we can pretend this to " + "be actually a review even if it is not "
                        + "your typical book review."),
                new ListField<>(spanList) };

        ITuple expectedTuple1 = new DataTuple(resultSchema, book1);
        List<ITuple> expectedResult = new ArrayList<>(1);
        expectedResult.add(expectedTuple1);

        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(TestUtils.equals(expectedResult, resultList));
    }


    /*
     * This case tests for the scenario when offset is some integer greater 
     * than 0 and greater than the actual number of results and join is 
     * performed.
     * Test result: An empty list.
     */
    @Test
    public void testOffsetGreaterThanNumberOfResults() throws Exception{
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90, 1, 10);

        Assert.assertEquals(0, resultList.size());
    }

    // ------------------------<Test cases for cursor.>------------------------
    /*
     * This case tests for the scenario when open and/or close is called twice 
     * and also when getNextTuple() is called when operator is closed.
     * Test result: Opening or closing the operator twice shouldn't result in 
     * any noticeable difference in operation. But, calling getNetTuple() when 
     * operator is closed should throw an exception.
     */
    @Test(expected = DataFlowException.class)
    public void testWhenOpenOrCloseIsCalledTwiceAndTryToGetNextTupleWhenClosed() throws Exception {
        List<ITuple> outerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        List<ITuple> innerTuples = JoinTestConstants.bookGroup1.subList(1, 5);
        
        JoinDistanceHelper.insertToOuter(outerTuples);
        JoinDistanceHelper.insertToInner(innerTuples);
           
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "typical", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "actually", conjunction);

        JoinDistancePredicate distancePredicate = new JoinDistancePredicate(
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 90);
        
        Join join = new Join(distancePredicate);
        join.setOuterInputOperator(keywordSourceOuter);
        join.setInnerInputOperator(keywordSourceInner);
        
        ITuple tuple;
        List<ITuple> resultList = new ArrayList<>();
        
        join.open();
        join.open();
        while ((tuple = join.getNextTuple()) != null) {
            resultList.add(tuple);
        }
        join.close();
        join.close();

        Assert.assertEquals(4, resultList.size());
        
        // this line should throw an exception because operator is already closed
        if ((tuple = join.getNextTuple()) != null) {
            resultList.add(tuple);
        }
    }

}