package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Shuying Lai
 * @author Zuozhi Wang
 */

public class RegexToGramQueryTranslatorTest {
	
	private void printTranslatorResult(String regex) {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		
		System.out.println("regex: "+regex);
		System.out.println("boolean expression: "+exactQuery.getLuceneQueryString());
		
		System.out.println("query tree: ");
		System.out.println(exactQuery.printQueryTree());
		
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		System.out.println("Simplified DNF: ");
		System.out.println(simplifiedDNF.printQueryTree());

		System.out.println();
	}
	
	@Test
	public void testEmptyRegex() {
		String regex = "";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
		printTranslatorResult(regex);
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testStarRegex() {
		String regex = "a*";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral1() {
		String regex = "abc";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"abc"}));
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral2() {
		String regex = "ab";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{}));
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral3() {
		String regex = "abcd";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"abc", "bcd"}));
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral4() {
		String regex = "ucirvine";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"uci", "cir", "irv", "rvi", "vin", "ine"}));
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral5() {
		String regex = "textdb";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		expectedQuery.operandSet.addAll(Arrays.asList(new String[]{"tex", "ext", "xtd", "tdb"}));
		printTranslatorResult(regex);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testCharClass1() {
		String regex = "[a-b][c-d][e-f]";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		GramBooleanQuery expectedQueryOrLevel = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		expectedQueryOrLevel.operandSet.addAll(Arrays.asList(
				new String[]{"ace", "acf", "bce", "bcf", "ade", "adf", "bde", "bdf"}));
		expectedQuery.subQuerySet.add(expectedQueryOrLevel);
		
		printTranslatorResult(regex);
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	// We can't write expectedQuery for the following expressions,
	// due to the complexity of the query itself,
	// and the boolean query is not simplified and contains lots of redundant information
	
	@Test
	public void testAlternate1() {
		printTranslatorResult("uci|ics");
	}
	
	@Test
	public void testAlternate2() {
		printTranslatorResult("data*(bcd|pqr)");
	}

	@Test
	public void testPlus1() {
		printTranslatorResult("abc+");
	}
	
	@Test
	public void testPlus2() {
		printTranslatorResult("abc+pqr+");
	}
	
	@Test
	public void testQuest1() {
		printTranslatorResult("abc?");
	}
	
	@Test
	public void testQuest2() {
		printTranslatorResult("abc?pqr?");
	}
	
	@Test
	// RE2J will simplify REPEAT to equivalent form with QUEST.
	// abc{1,3} will be simplified to abcc?c?
	public void testRepeat1() {
		printTranslatorResult("abc{1,3}");
	}
	
	@Test
	public void testCapture1() {
		printTranslatorResult("(abc)(qwer)");
	}
	
	@Test
	public void testRegexCropUrl() {
		printTranslatorResult("^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$");
	}	
	
}