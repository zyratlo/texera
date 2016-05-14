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
		GramBooleanQuery exactQuery = RegexToTrigram.translate("");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.ANY);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testStarRegex() {
		GramBooleanQuery exactQuery = RegexToTrigram.translate("a*");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.ANY);
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}

}
