package edu.uci.ics.textdb.dataflow.join;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

public class Join implements IOperator{
	
	public Join(ITuple outer, ITuple inner, IPredicate joinPredicate) {
		
	}

	@Override
	public void open() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
}
