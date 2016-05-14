package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Shuying Lai
 * @author Zuozhi Wang
 */

public class RegexToTrigramTest {
	
	@Test
	public void testEmptyRegex() {
		TrigramBooleanQuery exactQuery = RegexToTrigram.translate("");
		TrigramBooleanQuery expectedQuery = new TrigramBooleanQuery(TrigramBooleanQuery.ANY);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testStarRegex() {
		TrigramBooleanQuery exactQuery = RegexToTrigram.translate("a*");
		TrigramBooleanQuery expectedQuery = new TrigramBooleanQuery(TrigramBooleanQuery.ANY);
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}

}
