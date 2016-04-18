package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.reader.LuceneDataReader;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;

import edu.uci.ics.textdb.dataflow.regexmatch.RegexTestConstantsCorp;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexTestConstantStaff;

/**
 * Created by chenli on 3/25/16.
 * Modified by:
 * 		@zuozhi
 * 		@laisycs
 * 	
 * Unit test for RegexMatcher
 */
public class RegexMatcherTest {

	private RegexMatcher regexMatcher;
	private IDataWriter dataWriter;
	private IDataReader dataReader;
	private IDataStore dataStore;

	public void setUpPeople() throws Exception {
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR,
				TestConstants.SAMPLE_SCHEMA_PEOPLE);
		dataWriter = new LuceneDataWriter(dataStore);
		dataWriter.clearData();
		dataWriter.writeData(TestConstants.getSamplePeopleTuples());
	}

	public void setUpCorp() throws Exception {
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR,
				RegexTestConstantsCorp.SAMPLE_SCHEMA_CORP);
		dataWriter = new LuceneDataWriter(dataStore);
		dataWriter.clearData();
		dataWriter.writeData(RegexTestConstantsCorp.getSampleCorpTuples());
	}

	public void setUpStaff() throws Exception {
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR,
				RegexTestConstantStaff.SAMPLE_SCHEMA_STAFF);
		dataWriter = new LuceneDataWriter(dataStore);
		dataWriter.clearData();
		dataWriter.writeData(RegexTestConstantStaff.getSampleStaffTuples());

	}

	@After
	public void cleanUp() throws Exception {
		dataWriter.clearData();
	}

	
	@Test
	public void testGetNextTuplePeopleFirstName() throws Exception {
		setUpPeople();
		dataReader = new LuceneDataReader(dataStore,
				LuceneConstants.SCAN_QUERY, TestConstants.FIRST_NAME);

		String regex = "b.*"; // matches bruce and brad
		String fieldName = TestConstants.FIRST_NAME;
		IPredicate predicate = new RegexPredicate(regex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		List<ITuple> tuples = TestConstants.getSamplePeopleTuples();

		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		int numTuples = 0;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			boolean contains = TestUtils.contains(tuples, nextTuple, TestConstants.SAMPLE_SCHEMA_PEOPLE);
			Assert.assertTrue(contains);
			numTuples++;
		}
		Assert.assertEquals(2, numTuples);
		regexMatcher.close();
	}

	@Test
	public void testGetNextTupleCorpURL() throws Exception {
		setUpCorp();
		dataReader = new LuceneDataReader(dataStore,
				LuceneConstants.SCAN_QUERY, RegexTestConstantsCorp.URL);

		String urlRegex = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
		String fieldName = RegexTestConstantsCorp.URL;
		IPredicate predicate = new RegexPredicate(urlRegex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		List<ITuple> tuples = RegexTestConstantsCorp.getSampleCorpTuples();

		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		int numTuples = 0;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			boolean contains = TestUtils.contains(tuples, nextTuple, RegexTestConstantsCorp.SAMPLE_SCHEMA_CORP);
			Assert.assertTrue(contains);
			numTuples++;
		}
		Assert.assertEquals(3, numTuples);
		regexMatcher.close();
	}

	@Test
	public void testGetNextTupleCorpIP() throws Exception {
		setUpCorp();
		dataReader = new LuceneDataReader(dataStore,
				LuceneConstants.SCAN_QUERY, RegexTestConstantsCorp.IP_ADDRESS);

		String urlRegex = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
		String fieldName = RegexTestConstantsCorp.IP_ADDRESS;
		IPredicate predicate = new RegexPredicate(urlRegex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		List<ITuple> tuples = RegexTestConstantsCorp.getSampleCorpTuples();

		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		int numTuples = 0;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			boolean contains = TestUtils.contains(tuples, nextTuple, RegexTestConstantsCorp.SAMPLE_SCHEMA_CORP);
			Assert.assertTrue(contains);
			numTuples++;
		}
		Assert.assertEquals(3, numTuples);
		regexMatcher.close();
	}

	@Test
	public void testGetNextTupleStaffEmail() throws Exception {
		setUpStaff();
		dataReader = new LuceneDataReader(dataStore,
				LuceneConstants.SCAN_QUERY, RegexTestConstantStaff.EMAIL);

		String urlRegex = "^([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$";
		String fieldName = RegexTestConstantStaff.EMAIL;
		IPredicate predicate = new RegexPredicate(urlRegex, fieldName);
		ISourceOperator sourceOperator = new ScanBasedSourceOperator(dataReader);
		List<ITuple> tuples = RegexTestConstantStaff.getSampleStaffTuples();

		regexMatcher = new RegexMatcher(predicate, sourceOperator);
		regexMatcher.open();
		ITuple nextTuple = null;
		int numTuples = 0;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			boolean contains = TestUtils.contains(tuples, nextTuple, RegexTestConstantStaff.SAMPLE_SCHEMA_STAFF);
			Assert.assertTrue(contains);
			numTuples++;
		}
		Assert.assertEquals(4, numTuples);
		regexMatcher.close();
	}

}
