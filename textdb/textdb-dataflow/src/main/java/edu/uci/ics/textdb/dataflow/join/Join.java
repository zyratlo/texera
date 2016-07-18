package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.JoinPredicate;
/**
 * 
 * @author sripadks
 *
 */
public class Join implements IOperator{

	private IOperator outerOperator;
	private IOperator innerOperator;
	private JoinPredicate joinPredicate;
	// To indicate if next result from outer operator has to be obtained.
	private boolean getOuterOperatorNextTuple = true;			
	private ITuple outerTuple = null;
	private ITuple innerTuple = null;
	private List<ITuple> innerTupleList = new ArrayList<>();
	// Cursor to maintain the position of tuple to be obtained from innerTupleList.
	private Integer cursor = 0;
	// Value to be used as key in Span.
	private Integer spanKey = 0;

	/**
	 * This constructor is used to set the operators whose output is to be compared and joined and the 
	 * predicate which specifies the fields and constraints over which join happens.
	 * 
	 * @param outer is the outer operator producing the tuples
	 * @param inner is the inner operator producing the tuples
	 * @param joinPredicate is the predicate over which the join is made
	 */
	public Join(IOperator outerOperator, IOperator innerOperator, IPredicate joinPredicate) {
		this.outerOperator = outerOperator;
		this.innerOperator = innerOperator;
		this.joinPredicate = (JoinPredicate) joinPredicate;
	}

	@Override
	public void open() throws Exception, DataFlowException {
		try {
			outerOperator.open();
			innerOperator.open();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		// Load the inner tuple list into memory on open.
		while((innerTuple = innerOperator.getNextTuple()) != null) {
			innerTupleList.add(innerTuple);
		}
	}

	/**
	 * Gets the next tuple which is a joint of two tuples which passed the criteria set in the JoinPredicate.
	 * <br> Example in JoinPredicate.java
	 * 
	 * @return nextTuple
	 */
	@Override
	public ITuple getNextTuple() throws Exception {
		if(innerTupleList.isEmpty()) {
			return null;
		}

		if(getOuterOperatorNextTuple == true) {
			if((outerTuple = outerOperator.getNextTuple()) != null) {
				getOuterOperatorNextTuple = false;
			} else {
				return null;
			}
		}

		if (cursor < innerTupleList.size() -1 ) {
			innerTuple = innerTupleList.get(cursor);
			cursor++;
		} else if(cursor == innerTupleList.size() - 1) {
			innerTuple = innerTupleList.get(cursor);
			cursor = 0;
			getOuterOperatorNextTuple = true;
		}

		ITuple nextTuple = processTuples(outerTuple, innerTuple, joinPredicate);

		return nextTuple;
	}

	@Override
	public void close() throws Exception {
		try {
			outerOperator.close();
			innerOperator.close();

		} catch(Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		// Clear the inner tuple list from memory on close.
		innerTupleList.clear();
	}

	// Used to compare IDs of the tuples.
	private boolean compareId(ITuple outerTuple, ITuple innerTuple) {
		if(outerTuple.getField(joinPredicate.getidAttribute().getFieldName()).getValue()==
				innerTuple.getField(joinPredicate.getidAttribute().getFieldName()).getValue()) {
			return true;
		}
		return false;
	}

	// Process the tuples to get a tuple with join result if predicate is satisfied.
	private ITuple processTuples(ITuple outerTuple, ITuple innerTuple, JoinPredicate joinPredicate) throws Exception {
		ITuple nextTuple = null;
		List<Span> newJoinSpanList = new ArrayList<>();

		if(compareId(outerTuple, innerTuple)) {
			Integer indexOfInnerSpanList = innerTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST);
			Integer indexOfOuterSpanList = outerTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST);
			// If either/both tuples have no span information, return null.
			if(indexOfInnerSpanList == null || indexOfOuterSpanList == null) {
				return null;
			}

			List<Span> innerSpanList = null;
			List<Span> outerSpanList = null;
			// Check if both the fields obtained from the indexes are indeed of type ListField
			if(innerTuple.getField(indexOfInnerSpanList).getClass().equals(ListField.class)) {
				innerSpanList = (List<Span>) innerTuple.getField(indexOfInnerSpanList).getValue();
			}
			if(outerTuple.getField(indexOfOuterSpanList).getClass().equals(ListField.class)) {
				outerSpanList = (List<Span>) outerTuple.getField(indexOfOuterSpanList).getValue();
			}

			Iterator<Span> innerSpanIter = innerSpanList.iterator();
			Iterator<Span> outerSpanIter = outerSpanList.iterator();

			while(outerSpanIter.hasNext()) {
				Span outerSpan = outerSpanIter.next();
				// Check if the field matches the filed over which we want to join. If not return null.
				if(outerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
					while(innerSpanIter.hasNext()) {
						Span innerSpan = innerSpanIter.next();
						if(innerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
							Integer threshold = joinPredicate.getThreshold();
							if(Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold &&
									Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
								Integer newSpanStartIndex = Math.min(outerSpan.getStart(), innerSpan.getStart());
								Integer newSpanEndIndex = Math.max(outerSpan.getEnd(), innerSpan.getEnd());

								if(joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.STRING)||
										joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.TEXT)) {
									spanKey++;
									String fieldName = joinPredicate.getjoinAttribute().getFieldName();
									String fieldValue = (String) innerTuple.getField(fieldName).getValue();
									String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
									Span newSpan = new Span(
											joinPredicate.getjoinAttribute().getFieldName(), 
											newSpanStartIndex, newSpanEndIndex, 
											spanKey.toString(), newFieldValue);
									newJoinSpanList.add(newSpan);
								} else {
									throw new Exception("Fields other than \"STRING\" and \"TEXT\" are not supported by Join yet.");
								}
							}
						} else {
							return null;
						}
					}
				} else {
					return null;
				}
			}
		}

		Schema schema = innerTuple.getSchema();
		List<IField> fieldList = innerTuple.getFields();
		IField[] nextTupleField = new IField[fieldList.size()];

		for(Integer index = 0; index < nextTupleField.length - 1; index++) {
			nextTupleField[index] = fieldList.get(index);
		}

		nextTupleField[nextTupleField.length - 1] = new ListField<>(newJoinSpanList);
		nextTuple = new DataTuple(schema, nextTupleField);

		return nextTuple;
	}
}
