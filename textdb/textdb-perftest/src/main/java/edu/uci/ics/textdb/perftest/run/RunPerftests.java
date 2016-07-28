package edu.uci.ics.textdb.perftest.run;

/**
 * @author Hailey Pan 
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.perftest.keywordmatcher.*;
import edu.uci.ics.textdb.perftest.dictionarymatcher.*;

public class RunPerftests {

	/**
	 * Run all performance tests. 
	 * Make necessary changes for arguments, such as query file name and iteration number.
	 *
	 */
	public static void main(String[] args) {
		try {
			KeywordMatcherPerformanceTest.runTest("sample_queries.txt", 1);
			DictionaryMatcherPerformanceTest.runTest("sample_queries.txt", 1);

		} catch (StorageException | DataFlowException | IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
