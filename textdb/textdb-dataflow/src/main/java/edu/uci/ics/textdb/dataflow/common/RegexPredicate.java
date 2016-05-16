/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexToTrigram;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * @author sandeepreddy602
 *
 */
public class RegexPredicate implements IPredicate {

	private String regex;
	private Analyzer analyzer;
	private List<String> fields;
	private Query luceneQuery;
	private ISourceOperator sourceOperator;
	
	private enum RegexEngine {
		JavaRegex,
		RE2J
	}
	private RegexEngine regexEngine;
	

	public RegexPredicate(String regex, List<Attribute> attributeList, Analyzer analyzer, IDataStore dataStore)
			throws DataFlowException{
		try {
			this.regex = regex;
			this.analyzer = analyzer;		
			this.fields = attributeList.stream()
					.filter(attr -> (attr.getFieldType() == FieldType.TEXT || attr.getFieldType() == FieldType.STRING))
					.map(attr -> attr.getFieldName())
					.collect(Collectors.toList());
			
			// try to use RE2J
			try {
				com.google.re2j.Pattern.compile(regex);
				regexEngine = RegexEngine.RE2J;
				this.luceneQuery = generateQuery(this.regex, this.fields);	
			// if RE2J failes, try to use Java Regex
			} catch (com.google.re2j.PatternSyntaxException re2jException) {
				java.util.regex.Pattern.compile(regex);
				regexEngine = RegexEngine.JavaRegex;
				this.luceneQuery = generateScanQuery(this.fields);	
			}
			
			this.sourceOperator = new IndexBasedSourceOperator(new DataReaderPredicate(dataStore, luceneQuery));
		} catch (ParseException | java.util.regex.PatternSyntaxException e) {
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	private Query generateQuery(String regexStr, List<String> fields) throws ParseException {
		String[] fieldsArray = new String[fields.size()];
		QueryParser parser = new MultiFieldQueryParser(fields.toArray(fieldsArray), analyzer);
		String queryStr = RegexToTrigram.translate(regexStr);
		return parser.parse(queryStr);
	}
	
	private Query generateScanQuery(List<String> fields) throws ParseException {
		String[] fieldsArray = new String[fields.size()];
		QueryParser parser = new MultiFieldQueryParser(fields.toArray(fieldsArray), analyzer);
		return parser.parse(DataConstants.SCAN_QUERY);
	}

	public String getRegex() {
		return regex;
	}

	public ISourceOperator getSourceOperator() {
		return this.sourceOperator;
	}

	public List<String> getFields() {
		return this.fields;
	}


	/**
	 * This function returns a list of spans in the given tuple that match the
	 * regex For example, given tuple ("george watson", "graduate student", 23,
	 * "(949)888-8888") and regex "g[^\s]*", this function will return
	 * [Span(name, 0, 6, "g[^\s]*", "george
	 * watson"), Span(position, 0, 8, "g[^\s]*", "graduate student")]
	 * 
	 * @param tuple
	 *            document in which search is performed
	 * @return a list of spans describing the occurrence of a matching sequence
	 *         in the document
	 */
	public List<Span> computeMatches(ITuple tuple) {
		List<Span> spanList = new ArrayList<>();
		if (tuple == null) {
			return spanList; // empty array
		}
		for (String fieldName : fields) {
			IField field = tuple.getField(fieldName);
			String fieldValue = field.getValue().toString();
			if (fieldValue == null) {
				return spanList;
			} else {
				switch (regexEngine) {
				case JavaRegex:
					javaRegexMatch(fieldValue, fieldName, spanList);
					break;
				case RE2J:
					re2jRegexMatch(fieldValue, fieldName, spanList);
					break;
				}
			}
		}
		return spanList;
	}
	
	private void javaRegexMatch(String fieldValue, String fieldName, List<Span> spanList) {
		java.util.regex.Matcher javaMatcher = 
				java.util.regex.Pattern.compile(this.regex)
				.matcher(fieldValue);
		while (javaMatcher.find()) {
			spanList.add(new Span(fieldName, javaMatcher.start(), javaMatcher.end(), regex, fieldValue));
		}
	}
	
	private void re2jRegexMatch(String fieldValue, String fieldName, List<Span> spanList) {
		com.google.re2j.Matcher re2jMatcher = 
		com.google.re2j.Pattern.compile(this.regex)
		.matcher(fieldValue);
		while (re2jMatcher.find()) {
			spanList.add(new Span(fieldName, re2jMatcher.start(), re2jMatcher.end(), regex, fieldValue));
		}
	}

}
