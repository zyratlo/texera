package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
		if (TranslatorUtils.minLenOfString(literalList) < gramLength) {
			return;
		}
		if (literalList.size() == 0) {
			return;
		} else if (literalList.size() == 1) {
			this.addAndNode(literalList.get(0));
		} else {
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
	
	/**
	 * This function "AND"s two query trees together. <br>
	 * It also performs simple simplifications. <br>
	 * TODO: add more logic for more complicated and effecitve simplifications
	 * @param that GramBooleanQuery
	 * @return
	 */
	GramBooleanQuery computeConjunction (GramBooleanQuery that) {
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
			GramBooleanQuery query = new GramBooleanQuery(QueryOp.AND, gramLength);
			query.subQuerySet.add(this);
			query.subQuerySet.add(that);
			return query;
		}
	}
	
	GramBooleanQuery computeDisjunction (GramBooleanQuery that) {
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
			GramBooleanQuery query = new GramBooleanQuery(QueryOp.OR, gramLength);
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

	
	/**
	 * This returns a String that visualizes the query tree. <br>
	 */
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
	 * This returns a String that represents Lucene query. <br>
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
	
	public boolean isEmpty() {
		if (this.operandSet.size() > 0) {
			return false;
		}
		for (GramBooleanQuery subQuery : this.subQuerySet) {
			if (! subQuery.isEmpty()) {
				return false;
			}
		}
		return true;
	}
	
	// "AND" two DNF trees (trees are assumed to be in DNF form)
	// Apply distributive laws:
	// a AND (b OR c) = (a AND b) OR (a AND c)
	// (a OR b) AND (c OR d) = (a AND c) OR (a AND d) OR (b AND c) OR (c AND d)
	private static GramBooleanQuery andDNF(GramBooleanQuery left, GramBooleanQuery right) {
		if (left.isEmpty()) {
			return right;
		}
		if (right.isEmpty()) {
			return left;
		}
		GramBooleanQuery resultQuery = new GramBooleanQuery(QueryOp.OR);
		for (String leftOperand : left.operandSet) {
			for (String rightOperand : right.operandSet) {
				GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
				tempQuery.operandSet.add(leftOperand);
				tempQuery.operandSet.add(rightOperand);
				resultQuery.subQuerySet.add(tempQuery);
			}
			for (GramBooleanQuery rightSubQuery : right.subQuerySet) {
				GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
				tempQuery.operandSet.add(leftOperand);
				tempQuery.operandSet.addAll(rightSubQuery.operandSet);
				resultQuery.subQuerySet.add(tempQuery);
			}
		}
		for (GramBooleanQuery leftSubQuery : left.subQuerySet) {
			for (String rightOperand : right.operandSet) {
				GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
				tempQuery.operandSet.addAll(leftSubQuery.operandSet);
				tempQuery.operandSet.add(rightOperand);
				resultQuery.subQuerySet.add(tempQuery);
			}
			for (GramBooleanQuery rightSubQuery : right.subQuerySet) {
				GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
				tempQuery.operandSet.addAll(leftSubQuery.operandSet);
				tempQuery.operandSet.addAll(rightSubQuery.operandSet);
				resultQuery.subQuerySet.add(tempQuery);
			}
		}
		return resultQuery;
	}
	

	/**
	 * Simplify a tree, which is assumed to be already in DNF form
	 * Apply Absorption laws: a OR (a AND b) = a
	 * 
	 * Simplification is extremely important, because it removes lots of redundant information, 
	 * thus enabling comparison of two trees, 
	 *
	 * @param DNFQuery
	 * @return simplifiedDNFQuery
	 */
	public static GramBooleanQuery simplifyDNF(GramBooleanQuery query) {
		GramBooleanQuery result = new GramBooleanQuery(QueryOp.OR);
		result.operandSet.addAll(query.operandSet);
		
		Iterator<GramBooleanQuery> outerIterator = query.subQuerySet.iterator();
		OuterLoop:
		while (outerIterator.hasNext()) {
			GramBooleanQuery outerAndQuery = outerIterator.next();
			for (String operand : query.operandSet) {
				if (outerAndQuery.operandSet.contains(operand)) {
					continue OuterLoop;
				}
			}
			Iterator<GramBooleanQuery> innerIterator = query.subQuerySet.iterator();
			while (innerIterator.hasNext()) {
				GramBooleanQuery innerAndQuery = innerIterator.next();
				if (outerAndQuery != innerAndQuery) {
					if (outerAndQuery.operandSet.containsAll(innerAndQuery.operandSet)) {
						outerIterator.remove();
						continue OuterLoop;
					}				
				}
			}
			// if reach this code, then add a copy of it to result
			GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
			tempQuery.operandSet.addAll(outerAndQuery.operandSet);
			result.subQuerySet.add(tempQuery);
		}
		
		return query;
	}
	

	/**
	 * The query tree generated by the translator is messy with possibly lots of redundant information.
	 * This function transforms it into Disjunctive normal form (DNF), which is an OR of different ANDs.
	 * 
	 * To transform a tree to DNF form, the following laws are applied recursively from bottom to top:
	 * Associative laws: (a OR b) OR c = a OR (b OR c) = a OR b OR c, when transforming OR nodes,
	 * Distributive laws: a AND (b OR c) = (a AND b) OR (a AND c), when transforming AND nodes,
	 * 
	 * For each node, its children will be transformed to DNF form first, then 
	 * if it's OR, apply associative laws, if it's AND, apply distributive laws.
	 * Then recursively apply the same rules all the way up to the top node.
	 * 
	 * The result is NOT simplified. Must call simplifyDNF() to obtain the optimal tree.
	 * 
	 * @param query
	 * @return DNFQuery
	 */
	public static GramBooleanQuery toDNF(GramBooleanQuery query) {
		if (query.operator == QueryOp.AND) {
			GramBooleanQuery firstOrNode = new GramBooleanQuery(QueryOp.OR);
			if (query.operandSet.size() != 0) {
				GramBooleanQuery firstAndNode = new GramBooleanQuery(QueryOp.AND);
				firstAndNode.operandSet.addAll(query.operandSet);
				firstOrNode.subQuerySet.add(firstAndNode);
			}
			
			ArrayList<GramBooleanQuery> subDNFList = new ArrayList<>();
			for (GramBooleanQuery subQuery : query.subQuerySet) {
				subDNFList.add(toDNF(subQuery));
			}
			
			GramBooleanQuery result = subDNFList.stream().reduce(firstOrNode, (left, right) -> andDNF(left, right));
			return result;
		} else if (query.operator == QueryOp.OR) {
			GramBooleanQuery result = new GramBooleanQuery(QueryOp.OR);
			result.operandSet.addAll(query.operandSet);
			for (GramBooleanQuery subQuery : query.subQuerySet) {
				GramBooleanQuery newSubQuery = toDNF(subQuery);
				result.subQuerySet.addAll(newSubQuery.subQuerySet);
				result.operandSet.addAll(newSubQuery.operandSet);
			}
			return result;
		}
		
		// ANY or NONE, no need to simplify
		return query;
	}
	
}
