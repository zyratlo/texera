
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
    private ITuple dataTuple;
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
            dataTuple = operator.getNextTuple();
            fields = dataTuple.getFields();

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
            IField dataField = dataTuple.getField(fieldIndex);
            if (dataField instanceof StringField) {
                String fieldValue = ((StringField) dataField).getValue();

                // Get position of dict value in the field.
                if ((spanIndexVal = fieldValue.indexOf(dictValue, positionIndex)) != -1) {

                    // Increment positionIndex so that next search occurs from
                    // new positionIndex.
                    positionIndex = spanIndexVal + fieldValue.length();

                    Attribute attribute = dataTuple.getSchema().get(fieldIndex);
                    String fieldName = attribute.getFieldName();

                    // Creating a clone of the spanTuple and returning the
                    // cloned one,
                    // so that the changes are not reflected in the original
                    // spanTuple.
                    // The cloned SpanTuple is populated with the span related
                    // data.
                    ITuple spanTuple = dataTuple.clone();
                    spanTuple.addField(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE, new StringField(fieldName));
                    spanTuple.addField(SchemaConstants.SPAN_KEY_ATTRIBUTE, new StringField(dictValue));
                    spanTuple.addField(SchemaConstants.SPAN_BEGIN_ATTRIBUTE, new IntegerField(spanIndexVal));
                    spanTuple.addField(SchemaConstants.SPAN_END_ATTRIBUTE, new IntegerField(positionIndex - 1));
                    return spanTuple;

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
        else if ((dataTuple = operator.getNextTuple()) != null) {
            fieldIndex = 0;
            positionIndex = 0;
            fields = dataTuple.getFields();
            return getNextTuple();

        }
        // Get the next dictionary value
        else if ((dictValue = dict.getNextDictValue()) != null) {
            // At this point all the documents in the dataStore are scanned
            // and we need to scan them again for a different dictionary value
            fieldIndex = 0;
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
