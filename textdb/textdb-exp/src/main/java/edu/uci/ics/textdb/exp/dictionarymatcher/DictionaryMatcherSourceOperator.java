
package edu.uci.ics.textdb.exp.dictionarymatcher;


import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordSourcePredicate;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;


/**
 * @author Sudeep (inkudo)
 * @author Zuozhi Wang (zuozhi)
 * 
 */
public class DictionaryMatcherSourceOperator implements ISourceOperator {

    private ISourceOperator indexSource;
    
    private KeywordMatcherSourceOperator keywordSource;
    private DictionaryMatcher dictionaryMatcher;

    private Schema inputSchema;
    private Schema outputSchema;

    private Tuple sourceTuple;
    private String currentDictionaryEntry;

    private final DictionarySourcePredicate predicate;

    private int resultCursor;
    private int limit;
    private int offset;

    private Map<String, Tuple> resultMap; //To store results for earlier dictionary entries for CONJUNCTION and PHRASE.
    private Iterator resultIterator;

    private int cursor = CLOSED;  //Flag for computing matching results for CONJUNCTION and PHRASE.

    /**
     * Constructs a DictionaryMatcher with a dictionary predicate.
     *
     * Performs SUBSTRING_SCAN, PHRASE_INDEX, or CONJUNCTION_INDEX
     * depending on the dictionary predicate.
     *
     * DictionaryOperatorType.SUBSTRING_SCAN: <br>
     * Scan the tuples using ScanSourceOperator followed by a Dictionary Matcher. <br>
     * For each tuple, loop through the dictionary entries and generate results from
     * DictionaryMatcher. <br>
     *
     * DictionaryOperatorType.PHRASE_INDEX, CONJUNCTION_INDEX: <br>
     * Loop through the dictionary entries.
     * For each entry, use an index-based KeywordMatcher to get the matching results.
     * Maintain a HashMap </Tuple_ID, Tuple> to add all the matching results
     * into the spanlist of each input tuple.
     *
     * CONJUNCTION_INDEX corresponds to KeywordOperatorType.BASIC, which
     * performs keyword search on the document. The input query is
     * tokenized. The order of the tokens doesn't matter. <br>
     *
     * PHRASE_INDEX corresponds to KeywordOperatorType.PHRASE, which
     * performs phrase search on the document. The input query is
     * tokenized. The order of the tokens does matter. Stopwords are
     * treated as placeholders to indicate an arbitary token. <br>
     * 
     * @param predicate
     * 
     */
    public DictionaryMatcherSourceOperator(DictionarySourcePredicate predicate) {

        this.resultCursor = -1;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.predicate = predicate;

    }


    /**
     * @about Opens dictionary matcher. Must call open() before calling
     *        getNextTuple().
     */
    @Override
    public void open() throws DataFlowException {
        try {
            currentDictionaryEntry = predicate.getDictionary().getNextEntry();

            if (currentDictionaryEntry == null) {
                throw new DataFlowException("Dictionary is empty");
            }

            if (predicate.getKeywordMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED || predicate.getKeywordMatchingType() == KeywordMatchingType.REGEX) {

                // For Substring matching and Regex matching, create a scan source operator followed by a dictionary matcher.

                indexSource = new ScanBasedSourceOperator(new ScanSourcePredicate(predicate.getTableName()));

                dictionaryMatcher = new DictionaryMatcher(new DictionaryPredicate(predicate.getDictionary(), predicate.getAttributeNames(),
                        predicate.getAnalyzerString(), predicate.getKeywordMatchingType(), predicate.getSpanListName()));

                dictionaryMatcher.setInputOperator(indexSource);
                dictionaryMatcher.open();
                outputSchema = dictionaryMatcher.getOutputSchema();

            } else {
                // For other keyword matching types (CONJUNCTION and PHRASE),
                // create an index-based keyword source operator.

                keywordSource = new KeywordMatcherSourceOperator(new KeywordSourcePredicate(
                        currentDictionaryEntry,
                        predicate.getAttributeNames(),
                        predicate.getAnalyzerString(),
                        predicate.getKeywordMatchingType(),
                        predicate.getTableName(),
                        predicate.getSpanListName()));
                keywordSource.open();

                // Other keyword matching types uses a KeywordMatcher, so the
                // output schema is the same as keywordMatcher's schema.

                inputSchema = keywordSource.getOutputSchema();
                outputSchema = keywordSource.getOutputSchema();
            }

        } catch (Exception e) {

            throw new DataFlowException(e.getMessage(), e);

        }
    }

    /**
     * Gets the next matched tuple. <br>
     * Returns the tuple with results in spanList. <br>
     */
    @Override
    public Tuple getNextTuple() throws TextDBException {

        if (resultCursor >= limit + offset - 1) {
            return null;
        }
        if (predicate.getKeywordMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED
                || predicate.getKeywordMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {

            // For each dictionary entry, get all results from KeywordMatcher.
            if(cursor == CLOSED){

                resultMap = new HashMap<>();
                computeMatchingResults(resultMap);
                resultIterator = resultMap.entrySet().iterator();

                cursor = OPENED;

            }

            while (true) {

                if (resultIterator.hasNext()) {
                    resultCursor++;
                    sourceTuple = ((Map.Entry<String, Tuple>) resultIterator.next()).getValue();
                    if (resultCursor >= offset) {
                              return sourceTuple;
                    }

                    continue;

                } else{
                    return null;
                }

            }
        }

        // Substring matching (based on scan)
        else {

            while(true) {

                if ((sourceTuple = dictionaryMatcher.getNextTuple()) != null) {
                    resultCursor++;
                    if(resultCursor >= offset) {
                        return sourceTuple;
                    }
                    continue;

                }else return null;
            }
        }
    }


    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return this.limit;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getOffset() {
        return this.offset;
    }

    /***
     *  Maintain a HashMap </Tuple_ID, Tuple> to compute all the keyword
     *  matching results for each tuple.
     *
     * @param resultMap
     */

    private void computeMatchingResults(Map<String, Tuple> resultMap){

        Tuple inputTuple;
        while(true) {
            while((inputTuple = keywordSource.getNextTuple()) != null){

                String key = inputTuple.getField(SchemaConstants._ID).getValue().toString();
                if(resultMap.containsKey(key)) {

                    ListField<Span> spanListField = resultMap.get(key).getField(predicate.getSpanListName());
                    List<Span> spanList = spanListField.getValue();
                    spanList.addAll((List<Span>) inputTuple.getField(predicate.getSpanListName()).getValue());

                }else {

                    resultMap.put(key, inputTuple);
                }

            }

            if ((currentDictionaryEntry = predicate.getDictionary().getNextEntry()) == null) {
                 return;
            }

            keywordSource.close();

            KeywordSourcePredicate keywordSourcePredicate = new KeywordSourcePredicate(currentDictionaryEntry,
                    predicate.getAttributeNames(),
                    predicate.getAnalyzerString(), predicate.getKeywordMatchingType(),
                    predicate.getTableName(),
                    predicate.getSpanListName());

            keywordSource = new KeywordMatcherSourceOperator(keywordSourcePredicate);
            keywordSource.open();

        }

    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            if (keywordSource != null) {
                keywordSource.close();
            }
            if (indexSource != null) {
                indexSource.close();
            }

            if (dictionaryMatcher != null){
                dictionaryMatcher.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    public DictionaryPredicate getPredicate() {
        return this.predicate;
    }
    
}
