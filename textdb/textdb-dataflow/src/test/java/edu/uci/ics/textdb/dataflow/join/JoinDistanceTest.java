package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.DataConstants;
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
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.fuzzytokenmatcher.FuzzyTokenMatcher;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.relation.RelationManager;
import edu.uci.ics.textdb.storage.writer.DataWriter;
import junit.framework.Assert;

/**
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

    // writes the test tables before ALL tests
    @BeforeClass
    public static void setup() throws TextDBException {
        JoinDistanceHelper.createTestTables();
    }
    
    // deletes the test tables after ALL tests
    @AfterClass
    public static void cleanUp() throws TextDBException {
        JoinDistanceHelper.deleteTestTables();
    }
    
    // clear the test tables after EACH test
    @After
    public void clearTables() throws TextDBException {
        JoinDistanceHelper.clearTestTables();
    }
    
    // This case tests for scenario when the IDs of the documents don't match.
    // Test result: The list of result returned is empty.
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
    
    // This case tests for the scenario when the IDs of the documents match,
    // fields to join match and the difference of keyword spans is within
    // the given span threshold.
    // e.g.
    // [<11, 18>]
    // [<27, 33>]
    // threshold = 20
    // [           ]
    //          [          ]
    // <-------->
    //             <-------> (within threshold)
    // Test result: The list contains a tuple with all the fields and a span
    // list consisting of the joined span. The joined span is made up of the
    // field name, start and stop index (computed as <min(span1 spanStartIndex,
    // span2 spanStartIndex), max(span1 spanEndIndex, span2 spanEndIndex)>)
    // key (combination of span1 key and span2 key) and value (combination of
    // span1 value and span2 value).
    // [<11, 33>]
    @Test
    public void testIdsMatchFieldsMatchSpanWithinThreshold() throws Exception {
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
                
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "writer", conjunction);
        
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema spanSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 33, "special_writer", "special kind of " + "writer");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        
        ITuple expectedTuple = new DataTuple(spanSchema, book1);
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
                
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "takes a special kind of writer", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        
        Schema spanSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
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
        
        ITuple expectedTuple = new DataTuple(spanSchema, book1);
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "gastrointestinal tract", phrase);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "tract interesting", phrase);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema spanSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
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
        ITuple expectedTuple = new DataTuple(spanSchema, book1);
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
        
        KeywordMatcherSourceOperator keywordSourceOuter = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_OUTER, "special", conjunction);
        KeywordMatcherSourceOperator keywordSourceInner = 
                JoinDistanceHelper.getKeywordSource(BOOK_TABLE_INNER, "special", conjunction);
 
        List<ITuple> resultList = JoinDistanceHelper.getJoinDistanceResults(keywordSourceOuter, keywordSourceInner, 
                JoinTestConstants.ID, JoinTestConstants.REVIEW, 20, Integer.MAX_VALUE, 0);
        
        Schema spanSchema = Utils.createSpanSchema(JoinTestConstants.BOOK_SCHEMA);
        List<Span> spanList = new ArrayList<>();

        Span span1 = new Span(JoinTestConstants.REVIEW, 11, 18, "special_special", "special");
        spanList.add(span1);

        IField[] book1 = { new IntegerField(52), new StringField("Mary Roach"),
                new StringField("Grunt: The Curious Science of Humans at War"), new IntegerField(288),
                new TextField("It takes a special kind " + "of writer to make topics ranging from death to our "
                        + "gastrointestinal tract interesting (sometimes "
                        + "hilariously so), and pop science writer Mary Roach is " + "always up to the task."),
                new ListField<>(spanList) };
        ITuple expectedTuple = new DataTuple(spanSchema, book1);
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
                
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
        JoinDistanceHelper.insertToOuter(JoinTestConstants.bookGroup1.get(1));
        JoinDistanceHelper.insertToInner(JoinTestConstants.bookGroup1.get(1));
                
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
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(1);
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
        List<ITuple> expectedResult = new ArrayList<>(1);
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
        ITuple originalTuple = JoinTestConstants.bookGroup1.get(1);
        
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

    
    
    

}