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
	
	@Test
	public void testLiteral1() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("abcd");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"abc", "bcd"}));
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral2() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("ucirvine");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"uci", "cir", "irv", "rvi", "vin", "ine"}));
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral3() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("textdb");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"tex", "ext", "xtd", "tdb"}));
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
}
