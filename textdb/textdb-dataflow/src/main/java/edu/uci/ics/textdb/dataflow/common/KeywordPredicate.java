package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 *  @author prakul
 *  @author Zhenfeng Qi
 *  @author Zuozhi Wang
 *
 */

/**
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate {

	private final List<Attribute> attributeList;
	private final String query;
	private final Query luceneQuery;
	private ArrayList<String> queryTokenList;
	private HashSet<String> queryTokenSet;
	private ArrayList<String> queryTokensWithStopwords;
	private Analyzer luceneAnalyzer;
	private IDataStore dataStore;
	private KeywordMatchingType operatorType;

	/*
	 * query refers to string of keywords to search for. For Ex. New york if
	 * searched in TextField, we would consider both tokens New and York; if
	 * searched in String field we search for Exact string.
	 */
	public KeywordPredicate(String query, List<Attribute> attributeList, KeywordMatchingType operatorType,
			Analyzer luceneAnalyzer, IDataStore dataStore) throws DataFlowException {
		try {
			this.query = query;
			this.queryTokenList = Utils.tokenizeQuery(luceneAnalyzer, query);
			this.queryTokenSet = new HashSet<>(this.queryTokenList);
			this.queryTokensWithStopwords = Utils.tokenizeQueryWithStopwords(query);
			
			this.attributeList = attributeList;
			this.operatorType = operatorType;
			this.dataStore = dataStore;
			
			this.luceneAnalyzer = luceneAnalyzer;
			this.luceneQuery = createLuceneQueryObject();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	/**
	 * Creates a Query object as a boolean Query on all attributes Example: For
	 * creating a query like (TestConstants.DESCRIPTION + ":lin" + " AND " +
	 * TestConstants.LAST_NAME + ":lin") we provide a list of AttributeFields
	 * (Description, Last_name) to search on and a query string (lin)
	 *
	 * @return Query
	 * @throws ParseException
	 */
	private Query createLuceneQueryObject() throws ParseException {
		Query query = null;
		if (this.operatorType == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
			query = buildConjunctionQuery();
		}
		if (this.operatorType == KeywordMatchingType.PHRASE_INDEXBASED) {
			query = buildPhraseQuery();
		}
		if (this.operatorType == KeywordMatchingType.SUBSTRING_SCANBASED) {
			query = buildScanQuery();
		}

		return query;
	}
	
	
	
	private Query buildConjunctionQuery() {
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		
		for (Attribute attribute : this.attributeList) {
			String fieldName = attribute.getFieldName();
			FieldType fieldType = attribute.getFieldType();
			
			if (fieldType == FieldType.STRING) {
				Query termQuery = new TermQuery(new Term(fieldName, this.query));
				booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
			}		
			if (fieldType == FieldType.TEXT) {
				BooleanQuery.Builder fieldQueryBuilder = new BooleanQuery.Builder();
				for (String token : this.queryTokenSet) {
					Query termQuery = new TermQuery(new Term(fieldName, token.toLowerCase()));
					fieldQueryBuilder.add(termQuery, BooleanClause.Occur.MUST);
				}
				booleanQueryBuilder.add(fieldQueryBuilder.build(), BooleanClause.Occur.SHOULD);
			}
			
			// if it's not TEXT or STRING, we don't send query here
		}
		
		return booleanQueryBuilder.build();	
	}
	
	
	private Query buildPhraseQuery() {
		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		
		
		for (Attribute attribute : this.attributeList) {
			String fieldName = attribute.getFieldName();
			FieldType fieldType = attribute.getFieldType();
			
			if (fieldType == FieldType.STRING) {
				Query termQuery = new TermQuery(new Term(fieldName, this.query));
				booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
			}		
			if (fieldType == FieldType.TEXT) {
				if (queryTokenList.size() == 1) {
					Query termQuery = new TermQuery(new Term(fieldName, this.query.toLowerCase()));
					booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
				} else {
					PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
					for (int i = 0; i < queryTokensWithStopwords.size(); i++) {
						if (! StandardAnalyzer.STOP_WORDS_SET.contains(queryTokensWithStopwords.get(i))) {
							phraseQueryBuilder.add(new Term(fieldName, queryTokensWithStopwords.get(i).toLowerCase()), i);
						}
					}
					PhraseQuery phraseQuery = phraseQueryBuilder.build();
					booleanQueryBuilder.add(phraseQuery, BooleanClause.Occur.SHOULD);
				}
			}
			
			// if it's not TEXT or STRING, we don't send query here
		}
		
		return booleanQueryBuilder.build();
	}
	
	
	private Query buildScanQuery() {
		return new MatchAllDocsQuery();
	}

	
	
	public KeywordMatchingType getOperatorType() {
		return operatorType;
	}

	public String getQuery() {
		return query;
	}

	public List<Attribute> getAttributeList() {
		return attributeList;
	}

	public Query getQueryObject() {
		return this.luceneQuery;
	}

	public ArrayList<String> getQueryTokenList() {
		return this.queryTokenList;
	}
	
	public HashSet<String> getQueryTokenSet() {
		return this.queryTokenSet;
	}
	
	public ArrayList<String> getQueryTokensWithStopwords() {
		return this.queryTokensWithStopwords;
	}

	public Analyzer getLuceneAnalyzer() {
		return luceneAnalyzer;
	}

	public DataReaderPredicate getDataReaderPredicate() {
		DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(this.dataStore, this.luceneQuery, this.query,
				this.luceneAnalyzer, this.attributeList);
		return dataReaderPredicate;
	}

}