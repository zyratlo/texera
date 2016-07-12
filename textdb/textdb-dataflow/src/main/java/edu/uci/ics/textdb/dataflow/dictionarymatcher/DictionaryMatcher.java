
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
    private Schema spanSchema;
    
    private ITuple currentTuple;
    private String currentDictEntry;

    private final DictionaryPredicate predicate;

    /**
     * Constructs a DictionaryMatcher with a dictionary predicate
     * @param predicate
     * 
     */
    public DictionaryMatcher(IPredicate predicate) {
        this.predicate = (DictionaryPredicate) predicate;
        this.spanSchema = Utils.createSpanSchema(this.predicate.getDataStore().getSchema());

    }

    /**
     * @about Opens dictionary matcher. Must call open() before calling getNextTuple().
     */
    @Override
    public void open() throws DataFlowException {
        try {	
        	currentDictEntry = predicate.getNextDictEntry();
            if (currentDictEntry == null) {
            	throw new DataFlowException("Dictionary is empty");
            }
            
            if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORD_PHRASE) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(currentDictEntry, predicate.getAttributeList(),
                        KeywordOperatorType.PHRASE, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
            } else if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORD_BASIC) {
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
     * @about Gets next matched tuple. <br>
     * 		  Returns the tuple with results in spanList. <br>
     * 
     * 		  Performs SCAN, KEYWORD_BASIC, or KEYWORD_PHRASE depends on the 
     * 		  dictionary predicate. <br> 
     * 
     *        DictionaryOperatorType.SCAN: <br>
     *        Scan the document using ScanSourceOperator. <br>
     *        For each tuple from the document, loop through the dictionary 
     *        and find results. <br> <br>
     *        
     *        DictionaryOperatorType.KEYWORD_BASIC, KEYWORD_PHRASE: <br>
     *        Use KeywordMatcher to find results. <br>
     *        
     *        KEYWORD_BASIC corresponds to KeywordOperatorType.BASIC, which
     *        performs keyword search on the document. The input query is tokenized.
     *        The order of the tokens doesn't matter. <br>
     *        
     *        KEYWORD_PHRASE corresponds to KeywordOperatorType.PHRASE, which
     *        performs phrase search on the document. The input query is tokenized.
     *        The order of the tokens does matter. Stopwords are treated as placeholders 
     *        to indicate an arbitary token. <br>
     *        
     */
    @Override
    public ITuple getNextTuple() throws Exception {
    	if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORD_PHRASE
    	||  predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORD_BASIC) {
    		// for each dictionary entry
    		// get all result from KeywordMatcher
    		
            if ((currentTuple = sourceOperator.getNextTuple()) != null) {
            	return currentTuple;
            }
            
			if ((currentDictEntry = predicate.getNextDictEntry()) == null) {
				return null;
			}
			
			KeywordOperatorType keywordOperatorType;
			if (predicate.getSourceOperatorType() == DataConstants.DictionaryOperatorType.KEYWORD_PHRASE) {
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
    		// for each tuple
    		// iterate through the dictionary
    		
    		if (currentTuple == null) {
    			if ((currentTuple = sourceOperator.getNextTuple()) == null) {
    				return null;
    			}
    		}
    		
    		if (currentDictEntry == null) {
    			predicate.resetDictCursor();
    			currentDictEntry = predicate.getNextDictEntry();
    			currentTuple = sourceOperator.getNextTuple();
    			return getNextTuple();
    		}
    		    		
    		ITuple result = matchTuple(currentDictEntry, currentTuple);
    		
    		currentDictEntry = predicate.getNextDictEntry();
    		
    		if (result == currentTuple) {
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
