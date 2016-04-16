
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * @author Sudeep [inkudo]
 *
 */
public class DictionaryMatcher implements IOperator {

	private IOperator operator;
	private IDictionary dict;

	public DictionaryMatcher(IDictionary dict, IOperator operator) {
		this.operator = operator;
		this.dict = dict;

	}

	@Override
	public void open() throws Exception {
		try {
			operator.open();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		return null;
	}

	@Override
	public void close() throws Exception {

	}
}
