
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordOperatorType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

/**
 * @author Sudeep [inkudo]
 * 
 */
public class DictionaryMatcher implements IOperator {

    private IOperator sourceOperator;
    private String dictionaryValue;
    private int positionIndex; // next position in the field to be checked.
    private int attributeIndex; // Index of the next field to be checked.
    private String spanFieldName;
    private ITuple dataTuple;
    private List<IField> fields;
    private Schema spanSchema;

    private String regex;
    private Pattern pattern;
    private Matcher matcher;
    private List<Span> spanList;
    private boolean isPresent;
    private String documentValue;
    private final DictionaryPredicate predicate;

    /**
     * 
     * @param predicate
     * 
     */
    public DictionaryMatcher(IPredicate predicate) {
        this.predicate = (DictionaryPredicate) predicate;
    }

    /**
     * @about Opens dictionary matcher. Initializes positionIndex and fieldIndex
     *        Gets first sourceoperator tuple and dictionary value.
     */
    @Override
    public void open() throws Exception {
        try {
            positionIndex = 0;
            attributeIndex = 0;
            dictionaryValue = predicate.getNextDictionaryValue();

            if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.PHRASEOPERATOR) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(dictionaryValue, predicate.getAttributeList(),
                        KeywordOperatorType.PHRASE, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
            } else {

                if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.SCANOPERATOR) {
                    sourceOperator = predicate.getScanSourceOperator();
                    sourceOperator.open();
                } else if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.KEYWORDOPERATOR) {
                    KeywordPredicate keywordPredicate = new KeywordPredicate(dictionaryValue,
                            predicate.getAttributeList(), KeywordOperatorType.BASIC, predicate.getAnalyzer(),
                            predicate.getDataStore());
                    sourceOperator = new KeywordMatcher(keywordPredicate);
                    sourceOperator.open();
                }

                dataTuple = sourceOperator.getNextTuple();
                fields = dataTuple.getFields();
                // Java regex is used to detect word boundaries for TextField
                // match.
                // '\b' is used to match the beginning and end of the word.
                regex = "\\b" + dictionaryValue.toLowerCase() + "\\b";
                pattern = Pattern.compile(regex);

                spanSchema = getSpanSchema(dataTuple.getSchema());

                spanList = new ArrayList<>();
                isPresent = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.PHRASEOPERATOR) {
            if ((dataTuple = sourceOperator.getNextTuple()) != null) {
                return dataTuple;
            } else if ((dictionaryValue = predicate.getNextDictionaryValue()) != null) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(dictionaryValue, predicate.getAttributeList(),
                        KeywordOperatorType.PHRASE, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
                return getNextTuple();
            }

            return null;
        }

        if (attributeIndex < predicate.getAttributeList().size()) {
            IField dataField = dataTuple.getField(predicate.getAttributeList().get(attributeIndex).getFieldName());
            String fieldValue = (String) dataField.getValue();

            // Lucene tokenizes TextField before indexing, but not StringField,
            // so while matching a dictionary value, the dictionary value should
            // be the same as a StringField, while the dictionary value can be a
            // substring of a TextField value in order to be a match.
            if (dataField instanceof TextField) {
                matcher = pattern.matcher(fieldValue.toLowerCase());
                // Get position of dictionary value in the field.
                while (matcher.find(positionIndex) != false) {
                    isPresent = true;
                    int spanStartPosition = matcher.start();

                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanStartPosition + dictionaryValue.length();
                    documentValue = fieldValue.substring(spanStartPosition, positionIndex);
                    spanFieldName = predicate.getAttributeList().get(attributeIndex).getFieldName();

                    addSpanToSpanList(spanFieldName, spanStartPosition, positionIndex, dictionaryValue, documentValue);
                }

            } else if (dataField instanceof StringField) {
                // Dictionary value should exactly match fieldValue for a
                // StringField
                if (fieldValue.equals(dictionaryValue)) {
                    isPresent = true;
                    int spanStartPosition = 0;
                    positionIndex = spanStartPosition + dictionaryValue.length();
                    documentValue = fieldValue.substring(spanStartPosition, positionIndex);
                    spanFieldName = predicate.getAttributeList().get(attributeIndex).getFieldName(); // attribute.getFieldName();

                    addSpanToSpanList(spanFieldName, spanStartPosition, positionIndex, dictionaryValue, documentValue);
                }
            }

            attributeIndex++;
            positionIndex = 0;
            return getNextTuple();

        } else if (attributeIndex == predicate.getAttributeList().size() && isPresent) {
            isPresent = false;
            positionIndex = 0;
            if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.KEYWORDOPERATOR) {
                List<IField> fieldlist = removeSpanFromTuple(fields);
                return Utils.getSpanTuple(fieldlist, spanList, spanSchema);
            } else {
                return Utils.getSpanTuple(fields, spanList, spanSchema);
            }

        } else if ((dataTuple = sourceOperator.getNextTuple()) != null) {
            attributeIndex = 0;
            positionIndex = 0;
            spanList.clear();

            fields = dataTuple.getFields();
            return getNextTuple();

        } else if ((dictionaryValue = predicate.getNextDictionaryValue()) != null) {
            // Get the next dictionary value
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            attributeIndex = 0;
            positionIndex = 0;
            spanList.clear();

            regex = "\\b" + dictionaryValue.toLowerCase() + "\\b";
            pattern = Pattern.compile(regex);

            if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.SCANOPERATOR) {
                sourceOperator.close();
                sourceOperator.open();
            } else if (predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.KEYWORDOPERATOR) {
                KeywordPredicate keywordPredicate = new KeywordPredicate(dictionaryValue, predicate.getAttributeList(),
                        KeywordOperatorType.BASIC, predicate.getAnalyzer(), predicate.getDataStore());
                sourceOperator = new KeywordMatcher(keywordPredicate);
                sourceOperator.open();
            }

            dataTuple = sourceOperator.getNextTuple();
            fields = dataTuple.getFields();
            return getNextTuple();
        }

        return null;
    }

    private void addSpanToSpanList(String fieldName, int start, int end, String key, String value) {
        Span span = new Span(fieldName, start, end, key, value);
        spanList.add(span);
    }

    private List<IField> removeSpanFromTuple(List<IField> fieldList) {
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.remove(fieldListDuplicate.size() - 1);
        return fieldListDuplicate;
    }

    private Schema getSpanSchema(Schema schema) {
        if (spanSchema == null || predicate.getSourceOperatorType() == DataConstants.SourceOperatorType.SCANOPERATOR) {
            spanSchema = Utils.createSpanSchema(dataTuple.getSchema());
        }
        return spanSchema;
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
