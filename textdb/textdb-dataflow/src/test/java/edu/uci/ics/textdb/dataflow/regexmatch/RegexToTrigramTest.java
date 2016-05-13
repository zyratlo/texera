package edu.uci.ics.textdb.dataflow.regexmatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class RegexToTrigramTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testEmptyString() {
		TrigramBooleanQuery exactQuery = RegexToTrigram.translate("");
		TrigramBooleanQuery expectedQuery = new TrigramBooleanQuery(2);
		
//		RegexMatcherTestHelper testHelper = new RegexMatcherTestHelper(TestConstants.SCHEMA_PEOPLE, data);
		Assert.assertTrue(exactQuery.toString() == expectedQuery.toString());
	}

}
