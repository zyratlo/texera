package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import edu.uci.ics.textdb.common.constants.DataConstants;


public class GramBooleanQuery {
	public enum QueryOp {
		NONE, // doesn't match any string
		ANY,  // matches any string
		
		AND,
		OR
	}
	QueryOp operator;
	List<String> operandList;
	List<GramBooleanQuery> subQueryList;
	
	private int gramLength;
	
	public GramBooleanQuery(QueryOp operator) {
		this(operator, 3); //Default gram length is 3
	}
	
	public GramBooleanQuery(QueryOp operator, int gramLength) {
		this.operator = operator;
		operandList = new ArrayList<String>();
		subQueryList = new ArrayList<GramBooleanQuery>();
		this.gramLength = gramLength;
	}
	
	/**
	 * This return a GramBooleanQuery's hash code. <br>
	 * It won't traverse the whole tree, instead, 
	 * it only calculates the hashcode of direct leafs. <br>
	 * 
	 */
	public int hashCode() {
		int hashCode = operator.toString().hashCode();
		for (String s : operandList) {
			hashCode = hashCode ^ s.hashCode();
		}
		return hashCode;
	}
	
	public boolean equals(Object compareTo) {
		if (! (compareTo instanceof GramBooleanQuery)) {
			return false;
		}
		
		GramBooleanQuery query = (GramBooleanQuery) compareTo;
		if (this.operator != query.operator
			|| this.operandList.size() != query.operandList.size()
			|| this.subQueryList.size() != query.subQueryList.size()) {
			return false;
		}
		
		Set<String> operandSet = new HashSet<String>(this.operandList);
		if (!operandSet.equals(new HashSet<String>(query.operandList))) {
			return false;
		}
		
		Set<GramBooleanQuery> subQuerySet = new HashSet<GramBooleanQuery>(this.subQueryList);
		if (!subQuerySet.equals(new HashSet<GramBooleanQuery>(query.subQueryList))) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * This methods takes a list of strings and adds them to the query tree. <br>
	 * For example, if the list is {abcd, wxyz}, then: <br>
	 * trigrams({abcd, wxyz}) = trigrams(abcd) OR trigrams(wxyz) <br>
	 * OR operator is assumed for a list of strings. <br>
	 * @param list, a list of strings to be added into query.
	 */
	public void add(List<String> list) {
		addOrNode(list);
	}
	
	private void addOrNode(List<String> literalList) {
		GramBooleanQuery query = new GramBooleanQuery(GramBooleanQuery.QueryOp.OR);
		for (String literal : literalList) {
			query.addAndNode(literal);
		}
		this.subQueryList.add(query);
	}
	
	/**
	 * This method takes a single string and add it to the query tree. <br>
	 * For example: if the string is abcd, then: <br>
	 * trigrams(abcd) = abc AND bcd <br>
	 * AND operator is assumed for a single string. <br>
	 * @param literal
	 */
	private void addAndNode(String literal) {
		GramBooleanQuery query = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		for (String nGram: literalToNGram(literal)) {
			query.operandList.add(nGram);
		}
		this.subQueryList.add(query);
	}
	
	/**
	 * This function builds a list of N-Grams that a given literal contains. <br>
	 * If the length of the literal is smaller than N, it returns an empty list. <br>
	 * For example, for literal "textdb", its tri-gram list should be ["tex", "ext", "xtd", "tdb"]
	 * @param literal
	 * @return
	 */
	private List<String> literalToNGram(String literal) {
		ArrayList<String> nGrams = new ArrayList<>();
		if (literal.length() >= gramLength) {
			for (int i = 0; i <= literal.length()-gramLength; ++i) {
				nGrams.add(literal.substring(i, i+gramLength));
			}
		}
		return nGrams;
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
		if (operator == QueryOp.ANY) {
			return DataConstants.SCAN_QUERY;
		} else if (operator == QueryOp.NONE) {
			return "";
		} else {
			StringJoiner joiner =  new StringJoiner(
					(operator == QueryOp.AND) ? " AND " : " OR ");
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
