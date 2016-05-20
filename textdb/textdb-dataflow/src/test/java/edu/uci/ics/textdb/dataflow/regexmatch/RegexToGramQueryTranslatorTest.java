package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.Arrays;

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
//	
//	@Test
//	public void testLiteral1() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("abc");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"abc"}));
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testLiteral2() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("ab");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{}));
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testLiteral3() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("abcd");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"abc", "bcd"}));
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testLiteral4() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("ucirvine");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"uci", "cir", "irv", "rvi", "vin", "ine"}));
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testLiteral5() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("textdb");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"tex", "ext", "xtd", "tdb"}));
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testCharClass1() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("[a-b][c-d][e-f]");
//		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		GramBooleanQuery expectedQueryOrLevel = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
//		expectedQueryOrLevel.operandSet.addAll(Arrays.asList(
//				new String[]{"ace", "acf", "bce", "bcf", "ade", "adf", "bde", "bdf"}));
//		expectedQuery.subQuerySet.add(expectedQueryOrLevel);
//		
//		Assert.assertTrue(exactQuery.equals(expectedQuery));
//	}
//	
//	@Test
//	public void testAlternate1() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("uci|ics");
//		
//		System.out.println(exactQuery.getLuceneQueryString());
//		System.out.println(exactQuery.printQueryTree());
//	}
//	
//	@Test
//	public void testAlternate2() {
//		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("data*(bcd|pqr)");
//		
//		System.out.println(exactQuery.getLuceneQueryString());
//		System.out.println(exactQuery.printQueryTree());
//	}


	@Test
	public void testPlus1() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("abc+");

		System.out.println(exactQuery.getLuceneQueryString());
		System.out.println(exactQuery.printQueryTree());
	}
	
	
}
