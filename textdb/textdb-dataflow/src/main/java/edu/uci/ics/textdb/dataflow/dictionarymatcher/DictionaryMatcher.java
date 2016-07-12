
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordOperatorType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

/**
 * @author Sudeep (inkudo)
 * @author Zuozhi Wang (zuozhi)
 * 
 */
public class DictionaryMatcher implements IOperator {

    private IOperator sourceOperator;
    private String currentDictEntry;
    private Schema spanSchema;

    private final DictionaryPredicate predicate;

    /**
     * 
     * @param predicate
     * 
     */
    public DictionaryMatcher(IPredicate predicate) {
        this.predicate = (DictionaryPredicate) predicate;
        this.spanSchema = Utils.createSpanSchema(this.predicate.getDataStore().getSchema());

    }

    /**
     * @about Opens dictionary matcher. Initializes positionIndex and fieldIndex
     *        Gets first sourceoperator tuple and dictionary value.
     */
    @Override
    public void open() throws DataFlowException {
        try {	
            currentDictEntry = predicate.getNextDictionaryValue();
            if (currentDictEntry == null) {
            	throw new DataFlowException("Dictionary is empty");
            }

            if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.PHRASEOPERATOR) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictEntry, predicate.getAttributeList(),
                        KeywordOperatorType.PHRASE, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
            } else if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORDOPERATOR) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictEntry, predicate.getAttributeList(),
                        KeywordOperatorType.BASIC, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
            } else {
                sourceOperator = predicate.getScanSourceOperator();
                sourceOperator.open();
            }
            
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan, keyword or a phrase based search
     *        depending on the sourceoperator type, gets the dictionary value
     *        and scans the documents for matches Presently 2 types of
     *        KeywordOperatorType are supported:
     * 
     *        SourceOperatorType.SCANOPERATOR:
     * 
     *        Loop through the dictionary entries. For each dictionary entry,
     *        loop through the tuples in the operator. For each tuple, loop
     *        through the fields in the attributelist. For each field, loop
     *        through all the matches. Returns only one tuple per document. If
     *        there are multiple matches, all spans are included in a list. Java
     *        Regex is used to match word boundaries.
     * 
     *        Ex: If dictionary word is "Lin", and text is
     *        "Lin is Angelina's friend", matches should include Lin but not
     *        Angelina.
     * 
     *        SourceOperatorType.KEYWORDOPERATOR:
     * 
     *        Loop through the dictionary entries. For each dictionary entry,
     *        keywordmatcher's getNextTuple is called using
     *        KeyWordOperator.BASIC. Updates span information at the end of the
     *        tuple.
     * 
     *        SourceOperatorType.PHRASEOPERATOR:
     * 
     *        Loop through the dictionary entries. For each dictionary entry,
     *        keywordmatcher's getNextTuple is called using
     *        KeyWordOperator.PHRASE. The span returned is the span information
     *        provided by the keywordmatcher's phrase operator.
     * 
     */
    @Override
    public ITuple getNextTuple() throws Exception {
    	if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.PHRASEOPERATOR
    	||  predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORDOPERATOR) {
    		
    		ITuple nextTuple = null;
            if ((nextTuple = sourceOperator.getNextTuple()) != null) {
                return nextTuple;
            }
            
			if ((currentDictEntry = predicate.getNextDictionaryValue()) == null) {
				return null;
			}
			
			KeywordOperatorType keywordOperatorType;
			if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.PHRASEOPERATOR) {
				keywordOperatorType = KeywordOperatorType.PHRASE;
			} else {
				keywordOperatorType = KeywordOperatorType.BASIC;
			}
			
			KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictEntry, predicate.getAttributeList(),
					keywordOperatorType, predicate.getAnalyzer(), predicate.getDataStore());
			
			sourceOperator.close();
			sourceOperator = new KeywordMatcher(keywordPredicate);
			sourceOperator.open();
			
			return getNextTuple();
        }
    	else {
    		ITuple nextTuple = null;
    		if ((nextTuple = sourceOperator.getNextTuple()) == null) {
    			return null;
    		}
    		
    		ITuple result = matchTuple(currentDictEntry, nextTuple);
    		if (result == nextTuple) {
    			return getNextTuple();
    		} else {
    			return result;
    		}
    	}
    }
    
    /*
     * Match the key against the dataTuple.
     * if no match, returns the original dataTuple object,
     * if has match, return a new dataTuple with span list added
     */
    private ITuple matchTuple(String key, ITuple dataTuple) {
    	
    	List<Attribute> attributeList = predicate.getAttributeList();
    	List<Span> spanList = new ArrayList<>();
    	
    	for (Attribute attr : attributeList) {
    		String fieldName = attr.getFieldName();
    		String fieldValue = dataTuple.getField(fieldName).getValue().toString();
    		
    		// if attribute type is not TEXT, then key needs to match exact fieldValue
    		if (attr.getFieldType() != FieldType.TEXT) {
    			if (fieldValue.equals(key)) {
    				spanList.add(new Span(fieldName, 0, fieldValue.length(), key, fieldValue));
    			}
    		}
    		// if attribute type is TEXT, then key can match partial fieldValue
    		else {
    			String regex = "\\b" + key.toLowerCase() + "\\b";
    			Pattern pattern = Pattern.compile(regex);
    			Matcher matcher = pattern.matcher(fieldValue.toLowerCase());
    			while (matcher.find()) {
    				int start = matcher.start();
    				int end = matcher.end();
    				spanList.add(new Span(fieldName, start, end, key, fieldValue.substring(start, end)));
    			}
    		}
    	}
    	
    	if (spanList.size() == 0) {
    		return dataTuple;
    	} else {
    		return Utils.getSpanTuple(dataTuple.getFields(), spanList, this.spanSchema);
    	}
    }


    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            sourceOperator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
