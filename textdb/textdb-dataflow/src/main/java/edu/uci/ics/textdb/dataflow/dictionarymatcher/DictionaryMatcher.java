
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
    
    /**
     * 
     * @param dictionary 
     * @param operator
     * @param searchInAttributes --> The list of attributes to be searched in the tuple
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
            dictionaryValue = dictionary.getNextValue().toLowerCase();
            regex = "\\b" + dictionaryValue + "\\b";
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

    private Schema createSpanSchema() {
        List<Attribute> dataTupleAttributes = schema.getAttributes();
        Attribute[] spanAttributes = new Attribute[dataTupleAttributes.size() + 1];
        for (int count = 0; count < spanAttributes.length - 1; count++) {
            spanAttributes[count] = dataTupleAttributes.get(count);
        }
        spanAttributes[spanAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        Schema spanSchema = new Schema(spanAttributes );
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
     *           matches.
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (attributeIndex < searchInAttributes.size()) {
            IField dataField = dataTuple.getField(searchInAttributes.get(attributeIndex).getFieldName());

            if (dataField instanceof StringField) {
                String fieldValue = ((StringField) dataField).getValue();
                fieldValue = fieldValue.toLowerCase();

                matcher = pattern.matcher(fieldValue);
                // Get position of dict value in the field.
                if (matcher.find(positionIndex) != false) {

                    isPresent = true;
                    spanIndexValue = matcher.start();
                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexValue + dictionaryValue.length();

                    Attribute attribute = schema.getAttributes().get(attributeIndex);
                    spanFieldName = attribute.getFieldName();

                    addSpanToSpanList(spanFieldName, spanIndexValue, positionIndex, dictionaryValue, dictionaryValue);
                    return getNextTuple();

                } else {
                    // Increment the fieldIndex and call getNextTuple to search
                    // in next field
                    attributeIndex++;
                    positionIndex = 0;
                    return getNextTuple();
                }
            } else {
                // If fieldType is not String. Presently only supporting string
                // type in dictionary
                attributeIndex++;
                positionIndex = 0;
                return getNextTuple();
            }

        } else if (attributeIndex == searchInAttributes.size() && isPresent) {
            isPresent = false;
            positionIndex = 0;
            return getSpanTuple();
        }
        // Get the next document
        else if ((dataTuple = operator.getNextTuple()) != null) {
            attributeIndex = 0;
            positionIndex = 0;
            spanList.clear();

            fields = dataTuple.getFields();
            schema = dataTuple.getSchema();
            spanSchema = createSpanSchema();
            return getNextTuple();
        }
        // Get the next dictionary value
        else if ((dictionaryValue = dictionary.getNextValue()) != null) {
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            attributeIndex = 0;
            positionIndex = 0;

            dictionaryValue = dictionaryValue.toLowerCase();
            regex = "\\b" + dictionaryValue + "\\b";
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
