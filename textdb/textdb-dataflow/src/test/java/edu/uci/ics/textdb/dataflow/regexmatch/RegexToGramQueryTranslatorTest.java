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
		
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		System.out.println();
		System.out.println("----------------------------");
		
		System.out.println("regex: "+regex);
		System.out.println("boolean expression: "+simplifiedDNF.getLuceneQueryString());
		System.out.println();
		
//		System.out.println("original query tree: ");
//		System.out.println(exactQuery.printQueryTree());
//		
//		System.out.println("DNF: ");
//		System.out.println(dnf.printQueryTree());
		
		System.out.println("Simplified DNF: ");
		System.out.println(simplifiedDNF.printQueryTree());
		
		System.out.println("----------------------------");
		System.out.println();
	}
	
	@Test
	public void testEmptyRegex() {
		String regex = "";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testStarRegex() {
		String regex = "a*";
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral1() {
		String regex = "abc";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		expectedQuery.subQuerySet.add(GramBooleanQuery.newLeafNode("abc"));
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral2() {
		String regex = "ab";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral3() {
		String regex = "abcd";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedAndNode = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedAndNode.operandSet.addAll(Arrays.asList(new String[]{"abc", "bcd"}));
		expectedQuery.subQuerySet.add(expectedAndNode);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral4() {
		String regex = "abcde";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);

		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedAndNode = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedAndNode.operandSet.addAll(Arrays.asList(new String[]{"uci", "cir", "irv", "rvi", "vin", "ine"}));
		expectedQuery.subQuerySet.add(expectedAndNode);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testLiteral5() {
		String regex = "textdb";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);

		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedAndNode = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedAndNode.operandSet.addAll(Arrays.asList(new String[]{"tex", "ext", "xtd", "tdb"}));
		expectedQuery.subQuerySet.add(expectedAndNode);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testCharClass1() {
		String regex = "[a-b][c-d][e-f]";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
//		expectedQuery.operandSet.addAll(Arrays.asList(
//				new String[]{"ace", "acf", "bce", "bcf", "ade", "adf", "bde", "bdf"}));
		
		printTranslatorResult(regex);
		
		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}

	
	@Test
	public void testAlternate1() {
		String regex = "uci|ics";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
//		expectedQuery.operandSet.addAll(Arrays.asList(
//				new String[]{"uci", "ics"}));

		printTranslatorResult(regex);
		
		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testAlternate2() {
		String regex = "data*(bcd|pqr)";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedFirstAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedFirstAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"dat", "bcd"}));
		expectedQuery.subQuerySet.add(expectedFirstAnd);
		GramBooleanQuery expectedSecondAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedSecondAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"dat", "pqr"}));
		expectedQuery.subQuerySet.add(expectedSecondAnd);

		printTranslatorResult(regex);
		
		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));

	}

	@Test
	public void testPlus1() {
		String regex = "abc+";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedAndNode = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedAndNode.operandSet.addAll(Arrays.asList(new String[]{"abc"}));
		expectedQuery.subQuerySet.add(expectedAndNode);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testPlus2() {
		String regex = "abc+pqr+";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedFirstAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedFirstAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"abc", "cpq", "pqr"}));
		expectedQuery.subQuerySet.add(expectedFirstAnd);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));		
	}
	
	@Test
	public void testQuest1() {
		String regex = "abc?";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testQuest2() {
		String regex = "abc?pqr?";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedFirstAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedFirstAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"abp", "bpq"}));
		expectedQuery.subQuerySet.add(expectedFirstAnd);
		GramBooleanQuery expectedSecondAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedSecondAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"abc", "bcp", "cpq"}));
		expectedQuery.subQuerySet.add(expectedSecondAnd);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));		
	}
	
	@Test
	// RE2J will simplify REPEAT to equivalent form with QUEST.
	// abc{1,3} will be simplified to abcc?c?
	public void testRepeat1() {
		String regex = "abc{1,3}";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedAndNode = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedAndNode.operandSet.addAll(Arrays.asList(new String[]{"abc"}));
		expectedQuery.subQuerySet.add(expectedAndNode);
		
//		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}
	
	@Test
	public void testCapture1() {
		String regex = "(abc)(qwer)";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		GramBooleanQuery expectedFirstAnd = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
//		expectedFirstAnd.operandSet.addAll(Arrays.asList(
//				new String[]{"abc", "bcq", "cqw", "qwe", "wer"}));
		expectedQuery.subQuerySet.add(expectedFirstAnd);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));		
	}
	
	@Test
	public void testRegexCropUrl() {
		String regex = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
		
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate(regex);
		GramBooleanQuery dnf = GramBooleanQuery.toDNF(exactQuery);
		GramBooleanQuery simplifiedDNF = GramBooleanQuery.simplifyDNF(dnf);
		
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		
		printTranslatorResult(regex);

		Assert.assertTrue(simplifiedDNF.equals(expectedQuery));
	}	
	
}