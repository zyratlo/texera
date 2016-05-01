
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

/**
 * @author Sudeep [inkudo]
 * 
 */
public class DictionaryMatcher implements IOperator {

    private IOperator operator;
    private IDictionary dictionary;
    private String dictionaryValue;
    private int positionIndex; // next position in the field to be checked.
    private int attributeIndex; // Index of the next field to be checked.
    private int spanIndexValue; // Starting position of the matched dictionary
                                // string
    private String spanFieldName;
    private ITuple dataTuple;
    private List<IField> fields;
    private Schema schema;
    private Schema spanSchema;

    private String regex;
    private Pattern pattern;
    private Matcher matcher;
    private List<Attribute> searchInAttributes;
    private List<Span> spanList;
    private boolean isPresent;
    private String documentValue;

    /**
     * 
     * @param dictionary
     * @param operator
     * @param searchInAttributes
     *            --> The list of attributes to be searched in the tuple
     */
    public DictionaryMatcher(IDictionary dictionary, IOperator operator, List<Attribute> searchInAttributes) {
        this.operator = operator;
        this.dictionary = dictionary;
        this.searchInAttributes = searchInAttributes;
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
            operator.open();
            dictionaryValue = dictionary.getNextValue();

            // Java regex is used to detect word boundaries for TextField match.
            // '\b' is used to match the beginning and end of the word.
            regex = "\\b" + dictionaryValue.toLowerCase() + "\\b";
            pattern = Pattern.compile(regex);

            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();
            schema = dataTuple.getSchema();
            spanSchema = createSpanSchema();

            spanList = new ArrayList<>();
            isPresent = false;

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * 
     * @about Creating a new schema object, and adding SPAN_LIST_ATTRIBUTE to
     *        the schema. SPAN_LIST_ATTRIBUTE is of type List
     */
    private Schema createSpanSchema() {
        List<Attribute> dataTupleAttributes = schema.getAttributes();
        Attribute[] spanAttributes = new Attribute[dataTupleAttributes.size() + 1];
        for (int count = 0; count < spanAttributes.length - 1; count++) {
            spanAttributes[count] = dataTupleAttributes.get(count);
        }
        spanAttributes[spanAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        Schema spanSchema = new Schema(spanAttributes);
        return spanSchema;
    }

    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan based search, gets the dictionary
     *        value and scans all the documents for matches
     * 
     * @overview Loop through the dictionary entries. For each dictionary entry,
     *           loop through the tuples in the operator. For each tuple, loop
     *           through all the fields. For each field, loop through all the
     *           matches. Returns only one tuple per document. If there are
     *           multiple matches, all spans are included in a list. Java Regex
     *           is used to match word boundaries. Ex : If text is
     *           "Lin is Angelina's friend" and the dictionary word is "Lin",
     *           matches should include Lin but not Angelina.
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (attributeIndex < searchInAttributes.size()) {
            IField dataField = dataTuple.getField(searchInAttributes.get(attributeIndex).getFieldName());
            String fieldValue = (String) dataField.getValue();

            // Lucene tokenizes TextField before indexing, but not StrignField,
            // so while matching a dictionary value, the dictionary value should
            // be the same as a StringField, while the dictionary value can be a
            // substring of a TextField value in order to be a match.
            if (dataField instanceof TextField) {
                matcher = pattern.matcher(fieldValue.toLowerCase());
                // Get position of dict value in the field.
                while (matcher.find(positionIndex) != false) {
                    isPresent = true;
                    spanIndexValue = matcher.start();

                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexValue + dictionaryValue.length();
                    documentValue = fieldValue.substring(spanIndexValue, positionIndex);
                    spanFieldName = searchInAttributes.get(attributeIndex).getFieldName();

                    addSpanToSpanList(spanFieldName, spanIndexValue, positionIndex, dictionaryValue, documentValue);
                }

            } else if (dataField instanceof StringField) {
                // Dictionary value should exactly match fieldValue for a
                // StringField
                if (fieldValue.equals(dictionaryValue.toLowerCase())) {
                    isPresent = true;
                    spanIndexValue = 0;
                    positionIndex = spanIndexValue + dictionaryValue.length();
                    documentValue = fieldValue.substring(spanIndexValue, positionIndex);
                    spanFieldName = searchInAttributes.get(attributeIndex).getFieldName(); // attribute.getFieldName();

                    addSpanToSpanList(spanFieldName, spanIndexValue, positionIndex, dictionaryValue, documentValue);
                }
            }

            attributeIndex++;
            positionIndex = 0;
            return getNextTuple();

        } else if (attributeIndex == searchInAttributes.size() && isPresent) {
            isPresent = false;
            positionIndex = 0;
            return getSpanTuple();

        } else if ((dataTuple = operator.getNextTuple()) != null) {
            // Get the next document
            attributeIndex = 0;
            positionIndex = 0;
            spanList.clear();

            fields = dataTuple.getFields();
            schema = dataTuple.getSchema();
            spanSchema = createSpanSchema();
            return getNextTuple();

        } else if ((dictionaryValue = dictionary.getNextValue()) != null) {
            // Get the next dictionary value
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            attributeIndex = 0;
            positionIndex = 0;

            // dictionaryValueDup = dictionaryValue;
            regex = "\\b" + dictionaryValue.toLowerCase() + "\\b";
            pattern = Pattern.compile(regex);

            operator.close();
            operator.open();

            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();
            schema = dataTuple.getSchema();
            return getNextTuple();
        }

        return null;
    }

    private void addSpanToSpanList(String fieldName, int start, int end, String key, String value) {
        Span span = new Span(fieldName, start, end, key, value);
        spanList.add(span);
    }

    /**
     * @about Modifies schema, fields and creates a new span tuple
     */
    private ITuple getSpanTuple() {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fields);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(spanSchema, fieldsDuplicate);
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            operator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
