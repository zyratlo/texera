package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import edu.uci.ics.textdb.common.constants.DataConstants;


public class GramBooleanQuery {
	enum QueryOp {
		NONE, // doesn't match any string
		ANY,  // matches any string
		
		AND,
		OR
	}
	QueryOp operator;
	Set<String> operandSet;
	Set<GramBooleanQuery> subQuerySet;
	
	int gramLength;
	
	/**
	 * Constructs a GramBooleanQuery with default gram length 3. <br>
	 * @param operator
	 */
	GramBooleanQuery(QueryOp operator) {
		this(operator, 3);
	}
	
	GramBooleanQuery(QueryOp operator, int gramLength) {
		this.operator = operator;
		operandSet = new HashSet<String>();
		subQuerySet = new HashSet<GramBooleanQuery>();
		this.gramLength = gramLength;
	}
	
	
	/**
	 * This method takes a list of strings and adds them to the query tree. <br>
	 * For example, if the list is {abcd, wxyz}, then: <br>
	 * trigrams({abcd, wxyz}) = trigrams(abcd) OR trigrams(wxyz) <br>
	 * OR operator is assumed for a list of strings. <br>
	 * @param list, a list of strings to be added into query.
	 */
	void add(List<String> list) {
		addOrNode(list);
	}
	
	private void addOrNode(List<String> literalList) {
		if (literalList.size() == 0) {
			return;
		} else if (literalList.size() == 1) {
			this.addAndNode(literalList.get(0));
		} else {
			int minLength = 
				literalList.stream()
				.reduce(literalList.get(0), (a,b) -> (a.length() < b.length() ? a: b))
				.length();
			if (minLength < 3) {
				return ;
			}
			GramBooleanQuery query = new GramBooleanQuery(QueryOp.OR);
			for (String literal : literalList) {
				query.addAndNode(literal);
			}
			this.subQuerySet.add(query);
		}
	}
	
	/**
	 * This method takes a single string and adds it to the query tree. <br>
	 * The string is converted to multiple n-grams with an AND operator. <br>
	 * For example: if the string is abcd, then: <br>
	 * trigrams(abcd) = abc AND bcd <br>
	 * AND operator is assumed for a single string. <br>
	 * @param literal
	 */
	private void addAndNode(String literal) {
		if (literal.length() < gramLength) {
			return;
		} else if (literal.length() == gramLength) {
			this.operandSet.add(literal);
		} else {
			if (this.operator == QueryOp.AND) {
				this.operandSet.addAll(literalToNGram(literal));
			} else {
				GramBooleanQuery query = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
				query.operandSet.addAll(literalToNGram(literal));
				this.subQuerySet.add(query);
			}
		}
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
	
	
	GramBooleanQuery and (GramBooleanQuery that) {
		if (that.operator == QueryOp.ANY) {
			return this;
		}
		if (that.operator == QueryOp.NONE) {
			return that;
		}
		if (this.operator == QueryOp.ANY) {
			return that;
		}
		if (this.operator == QueryOp.NONE) {
			return this;
		}

		if (this.operator == QueryOp.AND && that.operator == QueryOp.AND) {
			this.operandSet.addAll(that.operandSet);
			this.subQuerySet.addAll(that.subQuerySet);
			return this;
		} else {
			GramBooleanQuery query = new GramBooleanQuery(QueryOp.AND, this.gramLength);
			query.subQuerySet.add(this);
			query.subQuerySet.add(that);
			return query;
		}
	}
	
	GramBooleanQuery or (GramBooleanQuery that) {
		if (that.operator == QueryOp.ANY) {
			return that;
		}
		if (that.operator == QueryOp.NONE) {
			return this;
		}
		if (this.operator == QueryOp.ANY) {
			return this;
		}
		if (this.operator == QueryOp.NONE) {
			return that;
		}
		
		if (this.operator == QueryOp.OR && that.operator == QueryOp.OR) {
			this.operandSet.addAll(that.operandSet);
			this.subQuerySet.addAll(that.subQuerySet);
			return this;
		} else {
			GramBooleanQuery query = new GramBooleanQuery(QueryOp.OR, this.gramLength);
			query.subQuerySet.add(this);
			query.subQuerySet.add(that);
			return query;
		}
	}
	
	/**
	 * This returns a GramBooleanQuery's hash code. <br>
	 * It won't traverse the whole tree, instead, 
	 * it only calculates the hashcode of direct leafs. <br>
	 * 
	 */
	@Override
	public int hashCode() {
		int hashCode = operator.toString().hashCode();
		for (String s : operandSet) {
			hashCode = hashCode ^ s.hashCode();
		}
		return hashCode;
	}
	
	/**
	 * This overrides "equals" function. Whenever a GramBooleanQUery 
	 * object is compared to another object, this function will be called. <br>
	 * It recursively traverses the query tree and compares 
	 * the set of sub-queries (order doesn't matter). <br>
	 * It internally uses a HashSet to compare sub-queries. <br>
	 */
	@Override
	public boolean equals(Object compareTo) {
		if (! (compareTo instanceof GramBooleanQuery)) {
			return false;
		}
		
		GramBooleanQuery that = (GramBooleanQuery) compareTo;
		if (this.operator != that.operator
			|| this.operandSet.size() != that.operandSet.size()
			|| this.subQuerySet.size() != that.subQuerySet.size()) {
			return false;
		}
		
		if (!this.operandSet.equals(that.operandSet)) {
			return false;
		}
		
		if (!this.subQuerySet.equals(that.subQuerySet)) {
			return false;
		}
		
		return true;
	}
	
//	@Override
//	public boolean equals(Object compareTo) {
//		if (! (compareTo instanceof GramBooleanQuery)) {
//			return false;
//		}
//		
//		GramBooleanQuery query = (GramBooleanQuery) compareTo;
//		if (this.operator != query.operator
//			|| this.operandList.size() != query.operandList.size()
//			|| this.subQueryList.size() != query.subQueryList.size()) {
//			return false;
//		}
//		
//		Set<String> operandSet = new HashSet<String>(this.operandList);
//		if (!operandSet.equals(new HashSet<String>(query.operandList))) {
//			return false;
//		}
//		
//		Set<GramBooleanQuery> subQuerySet = new HashSet<GramBooleanQuery>(this.subQueryList);
//		if (!subQuerySet.equals(new HashSet<GramBooleanQuery>(query.subQueryList))) {
//			return false;
//		}
//		
//		return true;
//	}
	
	
	String printQueryTree() {
		return queryTreeToString(this, 0, "  ");
	}
	
	private String queryTreeToString(GramBooleanQuery query, int indentation, String indentStr) {
		String s = "";
		
		for (int i = 0; i < indentation; i++) {
			s += indentStr;
		}
		s += query.operator.toString();
		s += "\n";
		
		indentation++;
		for (String operand : query.operandSet) {
			for (int i = 0; i < indentation; i++) {
				s += indentStr;
			}
			s += operand;
			s += "\n";
		}
		for (GramBooleanQuery subQuery : query.subQuerySet) {
			s += queryTreeToString(subQuery, indentation, indentStr);
		}
		indentation--;
		return s;
	}

	/**
	 * @return boolean expression 
	 */
	public String toString() {
		return this.getLuceneQueryString();
	}
	
	/**
	 * This function recursively connects 
	 *   operand in {@code operandList} and subqueries in {@code subqueryList} 
	 *   with {@code operator}. <br>
	 * It generates a string representing the query that can be directly parsed by Lucene.
	 * @return boolean expression
	 */
	public String getLuceneQueryString() {
		String luceneQueryString = toLuceneQueryString(this);
		if (luceneQueryString.isEmpty()) {
			return DataConstants.SCAN_QUERY;
		} else {
			return luceneQueryString;
		}
	}
	
	private static String toLuceneQueryString(GramBooleanQuery query) {
		if (query.operator == QueryOp.ANY) {
			return "";
		} else if (query.operator == QueryOp.NONE) {
			return "";
		} else {
			StringJoiner joiner =  new StringJoiner(
					(query.operator == QueryOp.AND) ? " AND " : " OR ");
			for (String operand : query.operandSet) {
				joiner.add(operand);
			}
			for (GramBooleanQuery subQuery : query.subQuerySet) {
				String subQueryStr = toLuceneQueryString(subQuery);
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
