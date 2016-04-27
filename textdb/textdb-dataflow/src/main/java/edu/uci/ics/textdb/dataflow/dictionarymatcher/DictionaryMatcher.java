
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
import edu.uci.ics.textdb.common.field.IntegerField;
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

    private String regex;
    private Pattern pattern;
    private Matcher matcher;
    private List<Attribute> attributes;

    public DictionaryMatcher(IDictionary dictionary, IOperator operator, List<Attribute> attributes) {
        this.operator = operator;
        this.dictionary = dictionary;
        this.attributes = attributes;
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

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
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
        // if (fieldIndex < fields.size()) {
        if (attributeIndex < attributes.size()) {
            IField dataField = dataTuple.getField(attributes.get(attributeIndex).getFieldName());

            if (dataField instanceof StringField) {
                String fieldValue = ((StringField) dataField).getValue();
                fieldValue = fieldValue.toLowerCase();

                matcher = pattern.matcher(fieldValue);
                // Get position of dict value in the field.
                if (matcher.find(positionIndex) != false) {

                    spanIndexValue = matcher.start();
                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexValue + dictionaryValue.length();

                    Attribute attribute = schema.getAttributes().get(attributeIndex);
                    spanFieldName = attribute.getFieldName();

                    return getSpanTuple();

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

        }
        // Get the next document
        else if ((dataTuple = operator.getNextTuple()) != null) {
            attributeIndex = 0;
            positionIndex = 0;
            fields = dataTuple.getFields();
            return getNextTuple();

        }
        // Get the next dictionary value
        else if ((dictionaryValue = dictionary.getNextValue()) != null) {
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            dictionaryValue = dictionaryValue.toLowerCase();
            regex = "\\b" + dictionaryValue + "\\b";
            pattern = Pattern.compile(regex);

            attributeIndex = 0;
            positionIndex = 0;

            operator.close();
            operator.open();
            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();
            return getNextTuple();
        }

        return null;

    }

    /**
     * @about Modifies schema, fields and creates a new span tuple
     */
    private ITuple getSpanTuple() {
        List<Attribute> attributesCopy = new ArrayList<>(schema.getAttributes());
        attributesCopy.add(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_KEY_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_BEGIN_ATTRIBUTE);
        attributesCopy.add(SchemaConstants.SPAN_END_ATTRIBUTE);

        List<IField> fieldListDuplicate = new ArrayList<>(fields);
        fieldListDuplicate.add(new StringField(spanFieldName));
        fieldListDuplicate.add(new StringField(dictionaryValue));
        fieldListDuplicate.add(new IntegerField(spanIndexValue));
        fieldListDuplicate.add(new IntegerField(positionIndex - 1));

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(new Schema(attributesCopy), fieldsDuplicate);
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
