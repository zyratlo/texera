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
 * @author laisycs
 * @author chenli
 * 	
 * Unit test for RegexMatcher
 */
public class RegexMatcherTest {
	private Schema createSpanSchema(Schema schema) {
    	List<Attribute> attributesCopy = new ArrayList<>(schema.getAttributes());
    	attributesCopy.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
    	return new Schema(attributesCopy);
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
		List<IField> fields = data.get(2).getFields();
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		//expected to match "george lin lin"
		spans.clear();
		spans.add(new Span(TestConstants.FIRST_NAME, 0, 6, "g[^\\s]*", "george lin lin"));
		spanField = new ListField<Span>(new ArrayList<Span>(spans));
		fields = data.get(3).getFields();
		fields.add(spanField);
		expected.add(new DataTuple(spanSchema, fields.toArray(new IField[fields.size()])));
		
		Assert.assertTrue(testHelper.matchExpectedResults(expected));
	}
	
//	@Test
//	public void testGetNextTupleCorpURL() throws Exception {
//		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
//		RegexMatcherTestHelper test = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);
//		
//		List<ITuple> expected = new ArrayList<ITuple>();
//		expected.add(data.get(1));
//		expected.add(data.get(2));
//		expected.add(data.get(3));
//		test.runTest("^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$",
//				RegexTestConstantsCorp.URL);
//		Assert.assertTrue(test.matchExpectedResults(expected));
//		
//		test.cleanUp();
//	}
//	
//	@Test
//	public void testGetNextTupleCorpIP() throws Exception {
//		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
//		RegexMatcherTestHelper test = new RegexMatcherTestHelper(RegexTestConstantsCorp.SCHEMA_CORP, data);
//		
//		List<ITuple> expected = new ArrayList<ITuple>();
//		expected.add(data.get(0));
//		expected.add(data.get(1));
//		expected.add(data.get(2));
//		test.runTest("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
//				RegexTestConstantsCorp.IP_ADDRESS);
//		Assert.assertTrue(test.matchExpectedResults(expected));
//		
//		test.cleanUp();
//	}
//	
//	@Test
//	public void testGetNextTupleStaffEmail() throws Exception {
//		List<ITuple> data = RegexTestConstantStaff.getSampleStaffTuples();
//		RegexMatcherTestHelper test = new RegexMatcherTestHelper(RegexTestConstantStaff.SCHEMA_STAFF, data);
//		
//		List<ITuple> expected = new ArrayList<ITuple>();
//		expected.add(data.get(0));
//		expected.add(data.get(1));
//		expected.add(data.get(2));
//		expected.add(data.get(3));
//		test.runTest("^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$",
//				RegexTestConstantStaff.EMAIL);
//		Assert.assertTrue(test.matchExpectedResults(expected));
//		
//		test.cleanUp();
//	}

}
