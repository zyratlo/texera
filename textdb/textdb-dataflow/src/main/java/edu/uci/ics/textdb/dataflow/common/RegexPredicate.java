/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import com.google.re2j.PublicRegexp;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
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


	private Matcher matcher;

	public RegexPredicate(String regex, List<Attribute> attributeList, Analyzer analyzer, IDataStore dataStore)
			throws DataFlowException {
		try {
			this.regex = regex;
			this.analyzer = analyzer;		
			this.fields = attributeList.stream().map(attr -> attr.getFieldName()).collect(Collectors.toList());
			this.luceneQuery = generateQuery(this.regex, this.fields);	
			this.sourceOperator = new IndexBasedSourceOperator(new DataReaderPredicate(dataStore, luceneQuery));
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	private Query generateQuery(String regexStr, List<String> fields) throws ParseException {
		String[] fieldsArray = new String[fields.size()];
		QueryParser parser = new MultiFieldQueryParser(fields.toArray(fieldsArray), analyzer);
		String queryStr = RegexToTrigram.translate(regexStr);
		return parser.parse(queryStr);
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

	public boolean satisfy(ITuple tuple) {
		if (tuple == null) {
			return false;
		}
		IField field = tuple.getField(fieldName);
		if (field instanceof StringField) {
			String fieldValue = ((StringField) field).getValue();
			if (fieldValue != null && fieldValue.matches(regex)) {
				return true;
			}
		}
		return false;
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
		IField field = tuple.getField(fieldName);
		if (field instanceof StringField || field instanceof TextField) {
			String fieldValue = ((StringField) field).getValue();
			if (fieldValue == null) {
				return spanList;
			} else {
				matcher = pattern.matcher(fieldValue);
				while (matcher.find()) {
					spanList.add(new Span(fieldName, matcher.start(), matcher.end(), regex, fieldValue));
				}
			}
		}

		return spanList;
	}

}
