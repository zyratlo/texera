package edu.uci.ics.textdb.dataflow.regexmatch;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Shuying Lai
 * @author Zuozhi Wang
 */

public class RegexToGramQueryTranslatorTest {
	private int indentation = 0;
	private String indentStr = "    ";
	
	private String printQueryTree(GramBooleanQuery query) {
		String s = "";
		for (int i = 0; i < indentation; i++) {
			s += indentStr;
		}
		s += query.operator.toString();
		s += "\n";
		
		indentation++;
		for (String operand : query.operandList) {
			for (int i = 0; i < indentation; i++) {
				s += indentStr;
			}
			s += operand;
			s += "\n";
		}
		for (GramBooleanQuery subQuery : query.subQueryList) {
			s += printQueryTree(subQuery);
		}
		indentation--;
		return s;
	}
	
	@Test
	public void testEmptyRegex() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);

		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}
	
	@Test
	public void testStarRegex() {
		GramBooleanQuery exactQuery = RegexToGramQueryTranslator.translate("a*");
		GramBooleanQuery expectedQuery = new GramBooleanQuery(GramBooleanQuery.QueryOp.ANY);
		
		Assert.assertTrue(exactQuery.equals(expectedQuery));
	}

}
