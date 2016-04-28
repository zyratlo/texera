
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import edu.uci.ics.textdb.api.common.Schema;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

/**
 * @author rajeshyarlagadda
 *
 */
public class DictionaryMatcherTest {

	private DictionaryMatcher dictionaryMatcher;
	private LuceneDataStore dataStore;
	private IDataWriter dataWriter;
	private IDataReader dataReader;
	private Analyzer analyzer;
	private Query query;

	@Before
	public void setUp() throws Exception {

		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
		analyzer = new StandardAnalyzer();
		dataWriter = new LuceneDataWriter(dataStore, analyzer);
		QueryParser queryParser = new QueryParser(TestConstants.ATTRIBUTES_PEOPLE[0].getFieldName(), analyzer);
		query = queryParser.parse(LuceneConstants.SCAN_QUERY);
		dataReader = new LuceneDataReader(dataStore, query);
		dataWriter.clearData();
		dataWriter.writeData(TestConstants.getSamplePeopleTuples());

	}

	@After
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

	public List<ITuple> getQueryResults(IDictionary dictionary, ISourceOperator sourceOperator, List<Attribute> attributes) throws Exception {

		dictionaryMatcher = new DictionaryMatcher(dictionary, sourceOperator, attributes);
		dictionaryMatcher.open();
		ITuple nextTuple = null;
		List<ITuple> results = new ArrayList<ITuple>();
		while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
			results.add(nextTuple);
		}
		dictionaryMatcher.close();
		return results;
	}

	/**
	 * Scenario S1:verifies GetNextTuple of Dictionary
	 */

	@Test
	public void testGetNextOfDictionaryItem() throws Exception {

		ArrayList<String> names = new ArrayList<String>(Arrays.asList("george", "lee"));
		IDictionary dictionary = new Dictionary(names);
		int numTuples = 0;
		String dictionaryItem;
		while ((dictionaryItem = dictionary.getNextValue()) != null) {
			boolean contains = TestUtils.contains(names, dictionaryItem);
			Assert.assertTrue(contains);
			numTuples++;
		}
		Assert.assertEquals(2, numTuples);

	}

	/**
	 * Scenario S-2(a):verifies GetNextTuple of DictionaryMatcher and single word
	 * queries in String Field
	 */

	@Test
	public void testSingleWordQueryInStringField() throws Exception {

		ArrayList<String> names = new ArrayList<String>(Arrays.asList("bruce"));
		IDictionary dictionary = new Dictionary(names);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		//create data tuple first
		List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 5, "bruce", "bruce");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        
		IField[] fields1 = { new StringField("bruce banner"), new StringField("john Lee"), new IntegerField(46),
				new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
				new TextField("Tall Angry"),new ListField<Span>(list) };
		ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		expectedResults.add(tuple1);
		List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
				TestConstants.DESCRIPTION_ATTR);
		
		List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
		boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
		Assert.assertTrue(contains);
	}
	
	/**
	 * Scenario S- 2(b):verifies GetNextTuple of DictionaryMatcher and single word
	 * queries in Text Field
	 */
	
	@Test
	public void testSingleWordQueryInTextField() throws Exception {

		ArrayList<String> names = new ArrayList<String>(Arrays.asList("tall"));
		IDictionary dictionary = new Dictionary(names);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		//create data tuple first
		List<Span> list = new ArrayList<Span>();
        Span span = new Span("description", 0, 4, "tall", "tall");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        
		IField[] fields1 = { new StringField("bruce banner"), new StringField("john Lee"), new IntegerField(46),
				new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
				new TextField("Tall Angry"),new ListField<Span>(list ) };
		IField[] fields2 = { new StringField("christian john wayne"), new StringField("rock bale"),
				new IntegerField(42), new DoubleField(5.99),
				new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("Tall Fair"), new ListField<Span>(list) };
		ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
		ITuple tuple2 = new DataTuple(new Schema(schemaAttributes), fields2);
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		expectedResults.add(tuple1);
		expectedResults.add(tuple2);
		List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
				TestConstants.DESCRIPTION_ATTR);
		
		List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
		boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
		Assert.assertTrue(contains);
	}

	/**
	 * Scenario S3:verifies ITuple returned by DictionaryMatcher and multiple
	 * word queries
	 */

	@Test
	public void testMultipleWordsQuery() throws Exception {

		ArrayList<String> names = new ArrayList<String>(Arrays.asList("george lin"));
		IDictionary dictionary = new Dictionary(names);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		//create data tuple first
		List<Span> list = new ArrayList<Span>();
        Span span = new Span("firstName", 0, 10, "george lin", "george lin");
        list.add(span);
        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        
        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
				new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
				new TextField("Short Angry"),new ListField<Span>(list)};
		ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		expectedResults.add(tuple1);
		List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
				TestConstants.DESCRIPTION_ATTR);
		
		List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
		boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
		Assert.assertTrue(contains);
	}
	
	
	/**
	 * Scenario S4:verifies: data source has multiple attributes, and an entity
	 * can appear in all the fields and multiple times.
	 */

	@Test
	public void testWordInMultipleFieldsQuery() throws Exception {


		ArrayList<String> names = new ArrayList<String>(Arrays.asList("Lin"));
		IDictionary dictionary = new Dictionary(names);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		//create data tuple first
		List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("firstName", 7, 10, "Lin", "Lin");
        Span span2 = new Span("firstName", 11, 14, "Lin", "Lin");
        Span span3 = new Span("lastName", 0, 3, "Lin", "Lin");
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
				new TextField("Short Angry"),new ListField<Span>(list)};
		ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
		List<ITuple> expectedResults = new ArrayList<ITuple>();
		expectedResults.add(tuple1);
		List<Attribute> attributes = Arrays.asList(TestConstants.FIRST_NAME_ATTR, TestConstants.LAST_NAME_ATTR,
				TestConstants.DESCRIPTION_ATTR);
		
		List<ITuple> returnedResults = getQueryResults(dictionary, sourceOperator, attributes);
		boolean contains = TestUtils.containsAllResults(expectedResults, returnedResults);
		Assert.assertTrue(contains);
	}
}
