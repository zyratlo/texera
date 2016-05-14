package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import edu.uci.ics.textdb.common.constants.DataConstants;

/**
 * 
 * Gram Query of OR and AND
 * 
 * {@code operandList} is a list of literals (strings) in this query.
 * {@code subQueryList} is a list of parenthesized RegexTrigramQuery.
 * {@code operator} is the operator connecting each literals in {@code operandList} and each subqueries in {@code subqueryList};
 * For example of trigram query, RegexTrigramQuery for regex "data(abc|bcd)" is "dat AND ata AND (abc OR bcd)"
 * The operand of this query is AND
 * operands = ["dat", "ata"]
 * subQueries = ["abc OR bcd"]
 * 
 * The gram query of a regex has two high-level layers:
 * First, conjunction of TrigramBooleanQuery of prefix, suffix, and exact.
 * Second, disjunction of TrigramBooleanQuery of each element respectively in prefix, suffix, and exact.
 * 
 * @Author Zuozhi Wang
 * @Author Shuying Lai
 * 
 */
public class GramBooleanQuery {
	public static final int NONE = 0;
	public static final int ANY  = 1;
	public static final int AND  = 2;
	public static final int OR   = 3;
	
	/**
	 * operator is NONE/ALL/AND/OR
	 */
	int operator;
	List<String> operandList;
	List<GramBooleanQuery> subQueryList;
	
	public GramBooleanQuery(int operator) {
		this.operator = operator;
		operandList = new ArrayList<String>();
		subQueryList = new ArrayList<GramBooleanQuery>();
	}
	
	public boolean equals(GramBooleanQuery query) {
		if (this.operator != query.operator
			|| this.operandList.size() != query.operandList.size() 
			|| this.subQueryList.size() != query.subQueryList.size()) {
			return false;
		}
		
		Set<String> operandSet = new HashSet<String>(this.operandList);
		if (!operandSet.equals(new HashSet<String>(query.operandList))) {
			return false;
		}
		
		if (this.subQueryList.size() == 0) {
			return true;
		}
		
		int[] used = new int[this.subQueryList.size()];
		return this.equalsHelper(query, used, 0);
	}
	/**
	 * This is a helper function called by {@code equals} function.
	 * It takes a DFS approach to recursively determine whether two {@code TrigramBooleanQuery} list contains same set of elements. 
	 * @param query
	 * @param isUsed
	 * @param index
	 * @return
	 */
	private boolean equalsHelper(GramBooleanQuery query, int[] isUsed, int index) {
		if (index == query.subQueryList.size()) {
			return true;
		}
		
		for (int i = 0; i < query.subQueryList.size(); i++) {
			if (isUsed[i] == 1) continue;
			if (this.subQueryList.get(index).equals(query.subQueryList.get(i))) {
				isUsed[i] = 1;
				if (equalsHelper(query, isUsed, index+1)){
					return true;
				} else {
					isUsed[i] = 0;
				}
			}
		}
		return false;
	}
	
	public void add(ArrayList<String> list) {
		addOrNode(list);
	}
	
	private void addOrNode(ArrayList<String> literalList) {
		GramBooleanQuery tbq = new GramBooleanQuery(GramBooleanQuery.OR);
		for (String literal : literalList) {
			tbq.addAndNode(literal);
		}
		this.subQueryList.add(tbq);
	}
	
	private void addAndNode(String literal) {
		GramBooleanQuery tbq = new GramBooleanQuery(GramBooleanQuery.AND);
		for (String trigram: literalToTrigram(literal)) {
			tbq.operandList.add(trigram);
		}
		this.subQueryList.add(tbq);
	}
	
	/**
	 * This function build a list of trigrams that a given literal contains.
	 * If the length of the literal is smaller than 3, it returns an empty list.
	 * For example, for literal "textdb", trigram list should be ["tex", "ext", "xtd", "tdb"]
	 * @param literal
	 * @return
	 */
	private List<String> literalToTrigram(String literal) {
		ArrayList<String> trigrams = new ArrayList<>();
		if (literal.length() >= 3) {
			for (int i = 0; i <= literal.length()-3; ++i) {
				trigrams.add(literal.substring(i, i+3));
			}
		}
		return trigrams;
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
		if (operator == GramBooleanQuery.ANY) {
			return DataConstants.SCAN_QUERY;
		} else if (operator == GramBooleanQuery.NONE) {
			return "";
		} else {
			StringJoiner joiner =  new StringJoiner(
					(operator == GramBooleanQuery.AND) ? " AND " : " OR ");

			for (String operand : operandList) {
				joiner.add(operand);
			}
			for (GramBooleanQuery subQuery : subQueryList) {
				String subQueryStr = subQuery.getQuery();
				if (! subQueryStr.equals("")) 
					joiner.add(subQueryStr);
			}
			
			if (joiner.length() == 0) {
				return "";
			} else {
				return "("+joiner.toString()+")";
			}
		}
	}
}
