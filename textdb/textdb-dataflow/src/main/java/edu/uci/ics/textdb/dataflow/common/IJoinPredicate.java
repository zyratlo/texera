package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * IJoinOperator is the interface for the classes implementing predicates for 
 * the Join Operator.
 * 
 * @author sripadks
 */
public interface IJoinPredicate {

	String getIDAttributeName();

	String getJoinAttributeName();

	ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema)
			throws Exception;
	
	Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataFlowException;
}