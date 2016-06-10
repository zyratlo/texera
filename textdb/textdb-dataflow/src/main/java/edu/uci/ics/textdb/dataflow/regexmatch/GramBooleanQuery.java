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

		LEAF, // leaf node, no child
		
		AND,
		OR
	}
	QueryOp operator;
	String leaf;
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
		leaf = "";
		subQuerySet = new HashSet<GramBooleanQuery>();
		this.gramLength = gramLength;
	}

	private static GramBooleanQuery newLeafNode(String literal) {
		GramBooleanQuery leafNode = new GramBooleanQuery(QueryOp.LEAF);
		leafNode.leaf = literal;
		return leafNode;
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
		GramBooleanQuery andNode;
		if (this.operator == QueryOp.AND) {
			andNode = this;
		} else {
			andNode = new GramBooleanQuery(QueryOp.AND);
			this.subQuerySet.add(andNode);
		}
		for (String gram : literalToNGram(literal)) {
			andNode.subQuerySet.add(newLeafNode(gram));
		}

		// if (literal.length() < gramLength) {
		// 	return;
		// } else if (literal.length() == gramLength) {
		// 	this.subQuerySet.add(newLeafNode(literal));
		// 	this.operandSet.add(literal);
		// } else {
		// 	if (this.operator == QueryOp.AND) {
		// 		this.operandSet.addAll(literalToNGram(literal));
		// 	} else {
		// 		GramBooleanQuery query = new GramBooleanQuery(GramBooleanQuery.QueryOp.AND);
		// 		query.operandSet.addAll(literalToNGram(literal));
		// 		this.subQuerySet.add(query);
		// 	}
		// }
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
	
	private void addToSubQuery(GramBooleanQuery that) {
		if (that.operator == QueryOp.LEAF) {
			this.subQuerySet.add(that);
		} else {
			this.subQuerySet.addAll(that.subQuerySet);
		}
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
			return deepCopy(this);
		}
		if (that.operator == QueryOp.NONE) {
			return deepCopy(that);
		}
		if (this.operator == QueryOp.ANY) {
			return deepCopy(that);
		}
		if (this.operator == QueryOp.NONE) {
			return deepCopy(this);
		}
		if ((this.operator == QueryOp.AND && that.operator == QueryOp.AND)
			|| (this.operator == QueryOp.AND && that.operator == QueryOp.LEAF)
			|| (this.operator == QueryOp.LEAF && that.operator == QueryOp.AND)) {
			GramBooleanQuery toReturn = new GramBooleanQuery(QueryOp.AND, gramLength);
			toReturn.addToSubQuery(deepCopy(this));
			toReturn.addToSubQuery(deepCopy(that));
			return toReturn;
		} else {
			GramBooleanQuery toReturn = new GramBooleanQuery(QueryOp.AND, gramLength);
			toReturn.subQuerySet.add(deepCopy(this));
			toReturn.subQuerySet.add(deepCopy(that));
			return toReturn;
		}
	}
	
	GramBooleanQuery computeDisjunction (GramBooleanQuery that) {
		if (that.operator == QueryOp.ANY) {
			return deepCopy(that);
		}
		if (that.operator == QueryOp.NONE) {
			return deepCopy(this);
		}
		if (this.operator == QueryOp.ANY) {
			return deepCopy(this);
		}
		if (this.operator == QueryOp.NONE) {
			return deepCopy(that);
		}
		if ((this.operator == QueryOp.OR && that.operator == QueryOp.OR)
				|| (this.operator == QueryOp.OR && that.operator == QueryOp.LEAF)
				|| (this.operator == QueryOp.LEAF && that.operator == QueryOp.OR)) {
				GramBooleanQuery toReturn = new GramBooleanQuery(QueryOp.OR, gramLength);
				toReturn.addToSubQuery(deepCopy(this));
				toReturn.addToSubQuery(deepCopy(that));
				return toReturn;
			} else {
				GramBooleanQuery toReturn = new GramBooleanQuery(QueryOp.OR, gramLength);
				toReturn.subQuerySet.add(deepCopy(this));
				toReturn.subQuerySet.add(deepCopy(that));
				return toReturn;
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
		if (operator == QueryOp.LEAF) {
			hashCode = hashCode ^ leaf.hashCode();
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
			// || this.operandSet.size() != that.operandSet.size()
			|| this.subQuerySet.size() != that.subQuerySet.size()) {
			return false;
		}

		if (this.operator == QueryOp.LEAF) {
			return this.leaf.equals(that.leaf);
		}
		
		// if (!this.operandSet.equals(that.operandSet)) {
		// 	return false;
		// }
		
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

		if (query.operator == QueryOp.LEAF) {
			s += query.leaf;
			s += "\n";
			return s; 
		}

		s += query.operator.toString();
		s += "\n";
		
		indentation++;
		// for (String operand : query.operandSet) {
		// 	for (int i = 0; i < indentation; i++) {
		// 		s += indentStr;
		// 	}
		// 	s += operand;
		// 	s += "\n";
		// }
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
		} else if (query.operator == QueryOp.LEAF) {
			return query.leaf;
		} else {
			StringJoiner joiner =  new StringJoiner(
					(query.operator == QueryOp.AND) ? " AND " : " OR ");
			// for (String operand : query.operandSet) {
			// 	joiner.add(operand);
			// }
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
		// if (this.operandSet.size() > 0) {
		// 	return false;
		// }
		if (this.operator == QueryOp.LEAF) {
			return this.leaf.isEmpty();
		}
		for (GramBooleanQuery subQuery : this.subQuerySet) {
			if (! subQuery.isEmpty()) {
				return false;
			}
		}
		return true;
	}
	
	public static GramBooleanQuery deepCopy(GramBooleanQuery query) {
		if (query.operator == QueryOp.ANY || query.operator == QueryOp.NONE) {
			return new GramBooleanQuery(query.operator);
		} else if (query.operator == QueryOp.LEAF) {
			return newLeafNode(query.leaf);
		} else {
			GramBooleanQuery toReturn = new GramBooleanQuery(query.operator);
			for (GramBooleanQuery subQuery : query.subQuerySet) {
				toReturn.subQuerySet.add(deepCopy(subQuery));
			}
			return toReturn;
		}
	}
	
	// "AND" two DNF tree
	// Apply distributive laws: 
	// (a OR b) AND (c OR d) --> (a AND c) OR (a AND d) OR (b AND c) OR (c AND d)
	private static GramBooleanQuery andDNF(GramBooleanQuery left, GramBooleanQuery right) {
		if (left.isEmpty()) {
			return right;
		}
		if (right.isEmpty()) {
			return left;
		}

		GramBooleanQuery resultQuery = new GramBooleanQuery(QueryOp.OR);

		for (GramBooleanQuery leftSubQuery : left.subQuerySet) {
			for (GramBooleanQuery rightSubQuery : right.subQuerySet) {
				System.out.println(leftSubQuery);
				System.out.println(rightSubQuery);
				
				GramBooleanQuery conjunction = leftSubQuery.computeConjunction(rightSubQuery);
				
				System.out.println(conjunction);

				resultQuery.subQuerySet.add(conjunction);
//				GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
//				tempQuery.operandSet.addAll(leftSubQuery.operandSet);
//				tempQuery.operandSet.addAll(rightSubQuery.operandSet);
//				resultQuery.subQuerySet.add(tempQuery);
			}
		}

		return resultQuery;
	}

	// After Transforming to DNF, apply Absorption laws to simplify it
	// a OR (a AND b) --> a
	// Tree must be already transformed to DNF before calling this function!
	public static GramBooleanQuery simplifyDNF(GramBooleanQuery query) {
		GramBooleanQuery result = new GramBooleanQuery(QueryOp.OR);

		Iterator<GramBooleanQuery> outerIterator = query.subQuerySet.iterator();
		OuterLoop:
		while (outerIterator.hasNext()) {
			GramBooleanQuery outerQuery = outerIterator.next();

			if (outerQuery.operator == QueryOp.LEAF) {
				result.subQuerySet.add(outerQuery);
			// else it's AND
			} else {
				Iterator<GramBooleanQuery> innerIterator = query.subQuerySet.iterator();
				while (innerIterator.hasNext()) {
					GramBooleanQuery innerQuery = innerIterator.next();
					if (outerQuery != innerQuery) {
						if (innerQuery.operator == QueryOp.LEAF) {
							if (outerQuery.subQuerySet.contains(innerQuery)) {
								continue OuterLoop;
							}
						} else {
							if (outerQuery.subQuerySet.containsAll(innerQuery.subQuerySet)) {
								continue OuterLoop;
							}
						}
					}
				}
			}

			// if reach this code, then add a copy of it to result
//			GramBooleanQuery tempQuery = new GramBooleanQuery(QueryOp.AND);
//			tempQuery.operandSet.addAll(outerAndQuery.operandSet);
//			result.subQuerySet.add(tempQuery);
			
			result.subQuerySet.add(outerQuery);

		}
		
		return query;
	}
	
	// Transform the GramBooleanQuery tree to Disjunctive normal form (DNF)
	// which is OR of different ANDs
	public static GramBooleanQuery toDNF(GramBooleanQuery query) {
		// if ANY or NONE, return itself
		if (query.operator == QueryOp.ANY || query.operator == QueryOp.NONE) {
			return query;
		}

		GramBooleanQuery result = new GramBooleanQuery(QueryOp.OR);

		if (query.operator == QueryOp.AND) {
			result = 
				query.subQuerySet.stream()
				.map(x -> toDNF(x))
				.reduce(new GramBooleanQuery(QueryOp.OR), (acc, next) -> andDNF(acc, next));

		} else if (query.operator == QueryOp.OR) {
			result = 
				query.subQuerySet.stream()
				.map(x -> toDNF(x))
				.reduce(result, (acc, next) -> {acc.subQuerySet.addAll(next.subQuerySet); return acc;});

		} else if (query.operator == QueryOp.LEAF) {
			result.subQuerySet.add(query);

		}

		return result;	
	}
	
	
}
