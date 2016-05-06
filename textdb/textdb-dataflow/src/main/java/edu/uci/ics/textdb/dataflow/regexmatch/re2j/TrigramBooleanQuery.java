package edu.uci.ics.textdb.dataflow.regexmatch.re2j;

import java.util.ArrayList;
import java.util.StringJoiner;

/*
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 * Trigram Query of OR and AND
 */
public class RegexTrigramQuery {
	// static final int or ENUM ?
	public static final int OR = 0;
	public static final int AND = 1;
	
	// private or public or no modifier?
	int operator;
	ArrayList<String> operands;
	// subQuerys or subQueries? better name?
	ArrayList<RegexTrigramQuery> subQueries;
	
	public RegexTrigramQuery(int operator) {
		this.operator = operator;
		operands = new ArrayList<String>();
		subQueries = new ArrayList<RegexTrigramQuery>();
	}
	
	public RegexTrigramQuery addSet(ArrayList<String> set) {
		RegexTrigramQuery query = new RegexTrigramQuery(RegexTrigramQuery.OR);
		return query;
	}
	
	// better name?
	public void add (ArrayList<String> str) {
		
	}
	
	public void add (String operand) {
		if (operand.length() == 3) {
			operands.add(operand);
		} else {
			
		}	
	}
	
	
	public String toString() {
		return this.getQuery();
	}
	
	public String getQuery() {
		StringJoiner joiner =  new StringJoiner(
				(operator == RegexTrigramQuery.AND) ? " AND " : " OR ");

		for (String operand : operands) {
			joiner.add(operand);
		}
		for (RegexTrigramQuery subQuery : subQueries) {
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
