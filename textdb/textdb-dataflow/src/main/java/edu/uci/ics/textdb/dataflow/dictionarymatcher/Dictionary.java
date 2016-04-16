package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.textdb.api.common.IDictionary;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary implements IDictionary {

	private int cursor = -1;
	private List<String> dict;

	public Dictionary() {
		dict = new ArrayList<String>(Arrays.asList("l5", "l2", "l4"));
		cursor = 0;
	}

	@Override
	// TODO
	public String getNextTuple() {
		if (cursor >= dict.size()) {
			cursor = 0;
			return null;
		}
		String dictval = dict.get(cursor++);
		return dictval;
	}

}
