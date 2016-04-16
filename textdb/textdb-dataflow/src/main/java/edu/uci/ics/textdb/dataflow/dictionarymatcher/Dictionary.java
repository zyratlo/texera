/**
 * 
 */
package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Sudeep [inkudo]
 *
 */
public class Dictionary {

	private int cursor = -1;

	public Dictionary() {
		List<String> dict = new ArrayList<String>(Arrays.asList("l5", "l2", "l4"));
		cursor = 0;
	}

}
