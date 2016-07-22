package edu.uci.ics.textdb.dataflow.source;

import java.util.Iterator;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

public class TupleSourceOperator implements ISourceOperator {
	
	Stream<ITuple> tupleStream;
	Iterator<ITuple> tupleIterator;
	
	public TupleSourceOperator(Stream<ITuple> tupleStream) {
		this.tupleStream = tupleStream;
	}

	@Override
	public void open() throws Exception {
		tupleIterator = tupleStream.iterator();
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		if (this.tupleIterator != null && this.tupleIterator.hasNext()) {
			return this.tupleIterator.next();
		}
		return null;
	}

	@Override
	public void close() throws Exception {
		if (this.tupleStream != null) {
			tupleStream.close();
		}
	}

}
