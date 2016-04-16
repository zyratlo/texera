
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

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
	private int positionIndex;
	private int fieldIndex;
	private int spanIndexVal;
	private ITuple spanTuple;

	public DictionaryMatcher(IDictionary dict, IOperator operator) {
		this.operator = operator;
		this.dict = dict;
	}

	@Override
	public void open() throws Exception {
		try {
			positionIndex = 0;
			fieldIndex = 0;
			operator.open();
			dictValue = dict.getNextTuple();
			spanTuple = operator.getNextTuple();

		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		IField field;
		if ((field = spanTuple.getField(fieldIndex)) != null) {
			if (field instanceof StringField) {
				String fieldValue = ((StringField) field).getValue();
				if ((spanIndexVal = fieldValue.indexOf(dictValue, positionIndex)) != -1) {

					positionIndex = spanIndexVal + fieldValue.length();

					Attribute attribute = spanTuple.getSchema().get(fieldIndex);
					String fieldName = attribute.getFieldName();

					spanTuple.addField(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE, new StringField(fieldName));
					spanTuple.addField(SchemaConstants.SPAN_KEY_ATTRIBUTE, new StringField(dictValue));
					spanTuple.addField(SchemaConstants.SPAN_BEGIN_ATTRIBUTE, new IntegerField(spanIndexVal));
					spanTuple.addField(SchemaConstants.SPAN_END_ATTRIBUTE, new IntegerField(positionIndex - 1));
					return spanTuple;

				} else {
					fieldIndex++;
					positionIndex = 0;
					return getNextTuple();
				}
			}
		} else if ((spanTuple = operator.getNextTuple()) != null) {
			fieldIndex = 0;
			positionIndex = 0;
			return getNextTuple();

		} else if ((dictValue = dict.getNextTuple()) != null) {
			fieldIndex = 0;
			positionIndex = 0;

			operator.close();
			operator.open();
			spanTuple = operator.getNextTuple();
			return getNextTuple();
		}

		return null;
	}

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
