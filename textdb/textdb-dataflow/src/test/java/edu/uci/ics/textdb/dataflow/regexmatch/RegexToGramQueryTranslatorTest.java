package edu.uci.ics.textdb.dataflow.regexmatch;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Shuying Lai
 * @author Zuozhi Wang
 */

public class RegexToGramQueryTranslatorTest {
	
//	@Test
//	public void testEmptyRegex() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
//
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testStarRegex() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("a*");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
	
	@Test
	public void testLiteral1() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("abcd");
		System.out.println(exactQuery.getLuceneQueryString());
		System.out.println(exactQuery.printQueryTree());

	}
	
	@Test
	public void testLiteral2() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("ucirvine");
		System.out.println(exactQuery.getLuceneQueryString());
		System.out.println(exactQuery.printQueryTree());

	}
	
	@Test
	public void testLiteral3() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("textdb");
		System.out.println(exactQuery.getLuceneQueryString());
		System.out.println(exactQuery.printQueryTree());

	}
	
}
