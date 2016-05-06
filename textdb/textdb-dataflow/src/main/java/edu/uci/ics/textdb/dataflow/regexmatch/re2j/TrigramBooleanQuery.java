package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * Trigram Query of OR and AND
 * 
 * {@code operandList} is a list of literals (strings) in this query. {@code subQueryList} is a list of parenthesized RegexTrigramQuery.
 * {@code operator} is the operator connecting each literals in {@code operandList} and each subquries in {@code subqueryList};
 * For example, RegexTrigramQuery for regex "data(abc|bcd)" is "dat AND ata AND (abc OR bcd)"
 * The operand of this query is AND
 * operands = ["dat", "ata"]
 * subQueries = ["abc OR bcd"]
 */
public class TrigramBooleanQuery {
	// static final int or ENUM ?
	public static final int OR = 0;
	public static final int AND = 1;
	
	/**
	 * operator is either AND or OR
	 */
	private int operator;
	private List<String> operandList;
	// subQuerys or subQueries? better name?
	private List<TrigramBooleanQuery> subQueryList;
	//abc and bcd and (abc or bcd)
	
	public TrigramBooleanQuery(int operator) {
		this.operator = operator;
		operandList = new ArrayList<String>();
		subQueryList = new ArrayList<TrigramBooleanQuery>();
	}
	
	public TrigramBooleanQuery addSet(ArrayList<String> set) {
		TrigramBooleanQuery query = new TrigramBooleanQuery(TrigramBooleanQuery.OR);
		return query;
	}
	
	// better name?
	public void add (ArrayList<String> str) {
		
	}
	
	public void add (String operand) {
		if (operand.length() == 3) {
			operandList.add(operand);
		} else {
			
		}	
	}
	
	/**
	 * @return boolean expression 
	 */
	public String toString() {
		return this.getQuery();
	}
	
	/**
	 * This function recursively connects 
	 *   operand in {@code operandList} and subqueries in {@code subqueryList} 
	 *   with {@code operator} 
	 * @return boolean expression
	 */
	public String getQuery() {
		StringJoiner joiner =  new StringJoiner(
				(operator == TrigramBooleanQuery.AND) ? " AND " : " OR ");

		for (String operand : operandList) {
			joiner.add(operand);
		}
		for (TrigramBooleanQuery subQuery : subQueryList) {
			joiner.add(subQuery.getQuery());
		}
		
		if (joiner.length() == 0) {
			// return null or empty string?
			return "";
		} else {
			return "("+joiner.toString()+")";
		}
	}
}
