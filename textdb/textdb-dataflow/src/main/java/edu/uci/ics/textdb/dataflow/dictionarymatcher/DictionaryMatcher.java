
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

/**
 * @author Sudeep [inkudo]
 * 
 */
public class DictionaryMatcher implements IOperator {

    private IOperator operator;
    private IDictionary dict;
    private String dictValue;
    private int positionIndex; // next position in the field to be checked.
    private int fieldIndex; // Index of the next field to be checked.
    private int spanIndexVal; // Starting position of the matched dictionary
                              // string
    private ITuple spanTuple;
    private List<IField> fields;

    public DictionaryMatcher(IDictionary dict, IOperator operator) {
        this.operator = operator;
        this.dict = dict;
    }

    /**
     * @about Opens dictionary matcher. Initializes positionIndex and fieldIndex
     *        Gets first sourceoperator tuple and dictionary value.
     */
    @Override
    public void open() throws Exception {
        try {
            positionIndex = 0;
            fieldIndex = 0;
            operator.open();
            dictValue = dict.getNextDictValue();
            spanTuple = operator.getNextTuple();
            fields = spanTuple.getFields();

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets next matched tuple. Returns a new span tuple including the
     *        span results. Performs a scan based search, gets the dictionary
     *        value and scans all the documents for matches
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (fieldIndex < fields.size()) {
            IField field = spanTuple.getField(fieldIndex);
            if (field instanceof StringField) {
                String fieldValue = ((StringField) field).getValue();

                // Get position of dict value in the field.
                if ((spanIndexVal = fieldValue.indexOf(dictValue, positionIndex)) != -1) {

                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexVal + fieldValue.length();

                    Attribute attribute = spanTuple.getSchema().get(fieldIndex);
                    String fieldName = attribute.getFieldName();

                    // Create a new span tuple with span results and return.
                    ITuple spanTupleCloned = spanTuple.clone();
                    spanTupleCloned.addField(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE, new StringField(fieldName));
                    spanTupleCloned.addField(SchemaConstants.SPAN_KEY_ATTRIBUTE, new StringField(dictValue));
                    spanTupleCloned.addField(SchemaConstants.SPAN_BEGIN_ATTRIBUTE, new IntegerField(spanIndexVal));
                    spanTupleCloned.addField(SchemaConstants.SPAN_END_ATTRIBUTE, new IntegerField(positionIndex - 1));
                    return spanTupleCloned;

                } else {
                    // Increment the fieldIndex and call getNextTuple to search
                    // in next field
                    fieldIndex++;
                    positionIndex = 0;
                    return getNextTuple();
                }
            } else {
                // If fieldType is not String. Presently only supporting string
                // type in dictionary
                fieldIndex++;
                positionIndex = 0;
                return getNextTuple();
            }

        }
        // Get the next document
        else if ((spanTuple = operator.getNextTuple()) != null) {
            fieldIndex = 0;
            positionIndex = 0;
            fields = spanTuple.getFields();
            return getNextTuple();

        }
        // Get the next dictionary value
        else if ((dictValue = dict.getNextDictValue()) != null) {
            fieldIndex = 0;
            positionIndex = 0;

            operator.close();
            operator.open();
            spanTuple = operator.getNextTuple();
            fields = spanTuple.getFields();
            return getNextTuple();
        }

        return null;
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
