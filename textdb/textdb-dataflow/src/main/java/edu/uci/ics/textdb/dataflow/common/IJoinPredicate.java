package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.dataflow.join.Join;

public interface IJoinPredicate {

	Attribute getIDAttribute();

	Attribute getJoinAttribute();

	ITuple joinTuples(Join join, ITuple outerTuple, ITuple innerTuple)
			throws Exception;
}