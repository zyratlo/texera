package edu.uci.ics.textdb.dataflow.regexmatch;

import edu.uci.ics.textdb.common.constants.DataConstants;

// dummy translator program, always return SCAN_QUERY
public class RegexToTrigram {
	public static String translate(String regex) {
		return DataConstants.SCAN_QUERY;
	}
}
