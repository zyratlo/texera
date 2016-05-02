package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;

/**
 * @author zuozhi
 * @author shuying
 * @author chenli
 * 	
 * Unit test for RegexMatcher
 */
public class RegexMatcherTest {
	private Schema createSpanSchema(Schema schema) {
    	List<Attribute> attributesCopy = new ArrayList<>(schema.getAttributes());
    	attributesCopy.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
    	return new Schema(attributesCopy.toArray(new Attribute[attributesCopy.size()]));
    }
	
	@Test
	public void testGetNextTuplePeopleFirstName() throws Exception {
		List<ITuple> data = TestConstants.getSamplePeopleTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(TestConstants.SCHEMA_PEOPLE, data);
		
		testHelper.runTest("g[^\\s]*", TestConstants.FIRST_NAME);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		
		//expected to match "brad lie angelina"
		Schema spanSchema = createSpanSchema(TestConstants.SCHEMA_PEOPLE);
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(TestConstants.FIRST_NAME, 11, 17, "g[^\\s]*", "brad lie angelina"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "george lin lin"
		spans.clear();
		spans.add(new Span(TestConstants.FIRST_NAME, 0, 6, "g[^\\s]*", "george lin lin"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(3).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(testHelper.matchExpectedResults(expected));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpURL() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);
		
		String query = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
		testHelper.runTest(query, RegexTestConstantsCorp.URL);
		
		List<ITuple> expected = new ArrayList<ITuple>();

		//expected to match "http://weibo.com"
		Schema spanSchema = createSpanSchema(RegexTestConstantsCorp.SCHEMA_CORP);
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsCorp.URL, 0, 16, query, "http://weibo.com"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "https://www.microsoft.com/en-us/"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.URL, 0, 32, query, "https://www.microsoft.com/en-us/"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(testHelper.matchExpectedResults(expected));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpIP() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);

		String query = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
		testHelper.runTest(query, RegexTestConstantsCorp.IP_ADDRESS);

		List<ITuple> expected = new ArrayList<ITuple>();
		
		//expected to match "66.220.144.0"
		Schema spanSchema = createSpanSchema(RegexTestConstantsCorp.SCHEMA_CORP);
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "66.220.144.0"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "180.149.134.141"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 15, query, "180.149.134.141"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "131.107.0.89"
		spans.clear();
		spans.add(new Span(RegexTestConstantsCorp.IP_ADDRESS, 0, 12, query, "131.107.0.89"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(2).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(testHelper.matchExpectedResults(expected));
		
		testHelper.cleanUp();
	}
	
	@Test
	public void testGetNextTupleStaffEmail() throws Exception {
		List<ITuple> data = RegexTestConstantStaff.getSampleStaffTuples();
		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(RegexTestConstantStaff.SCHEMA_STAFF, data);
		
		String query = "^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$";
		testHelper.runTest(query, RegexTestConstantStaff.EMAIL);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		
		//expected to match "66.220.144.0"
		Schema spanSchema = createSpanSchema(RegexTestConstantStaff.SCHEMA_STAFF);
		List<Span> spans = new ArrayList<Span>();
		spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 19, query, "k.bocanegra@uci.edu"));
		IField spanField = new ListField<Span>(new ArrayList<Span>(spans));
		List<IField> fields = new ArrayList<IField>(data.get(0).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "180.149.134.141"
		spans.clear();
		spans.add(new Span(RegexTestConstantStaff.EMAIL, 0, 18, query, "hwangl@ics.uci.edu"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = new ArrayList<IField>(data.get(1).getFields());
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(testHelper.matchExpectedResults(expected));
				
		testHelper.cleanUp();
	}

}
