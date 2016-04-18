package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.List;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.Assert;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexTestConstantsCorp;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexTestConstantStaff;

/**
 * @author zuozhi
 * @author laisycs
 * @author chenli
 * 	
 * Unit test for RegexMatcher
 */
public class RegexMatcherTest {
	
	
	@Test
	public void testGetNextTuplePeopleFirstName() throws Exception {
		List<ITuple> data = TestConstants.getSamplePeopleTuples();
		RegexMatcherTester test = new RegexMatcherTester(TestConstants.SAMPLE_SCHEMA_PEOPLE, data);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		expected.add(data.get(0));
		expected.add(data.get(2));
		test.runTest("b.*", TestConstants.FIRST_NAME);
		Assert.assertTrue(test.matchExpectedResults(expected));
		
		test.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpURL() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTester test = new RegexMatcherTester(RegexTestConstantsCorp.SAMPLE_SCHEMA_CORP, data);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		expected.add(data.get(1));
		expected.add(data.get(2));
		expected.add(data.get(3));
		test.runTest("^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$",
				RegexTestConstantsCorp.URL);
		Assert.assertTrue(test.matchExpectedResults(expected));
		
		test.cleanUp();
	}
	
	@Test
	public void testGetNextTupleCorpIP() throws Exception {
		List<ITuple> data = RegexTestConstantsCorp.getSampleCorpTuples();
		RegexMatcherTester test = new RegexMatcherTester(RegexTestConstantsCorp.SAMPLE_SCHEMA_CORP, data);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		expected.add(data.get(0));
		expected.add(data.get(1));
		expected.add(data.get(2));
		test.runTest("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
				RegexTestConstantsCorp.IP_ADDRESS);
		Assert.assertTrue(test.matchExpectedResults(expected));
		
		test.cleanUp();
	}
	
	@Test
	public void testGetNextTupleStaffEmail() throws Exception {
		List<ITuple> data = RegexTestConstantStaff.getSampleStaffTuples();
		RegexMatcherTester test = new RegexMatcherTester(RegexTestConstantStaff.SAMPLE_SCHEMA_STAFF, data);
		
		List<ITuple> expected = new ArrayList<ITuple>();
		expected.add(data.get(0));
		expected.add(data.get(1));
		expected.add(data.get(2));
		expected.add(data.get(3));
		test.runTest("^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$",
				RegexTestConstantStaff.EMAIL);
		Assert.assertTrue(test.matchExpectedResults(expected));
		
		test.cleanUp();
	}

}
