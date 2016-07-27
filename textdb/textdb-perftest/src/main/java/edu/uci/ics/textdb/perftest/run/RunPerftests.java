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
import edu.uci.ics.textdb.perftest.fuzzytokenmatcher.*;

public class RunPerftests {

	/**
	 * Run all performance tests make necessary changes for arguments
	 *
	 */
	public static void main(String[] args) {
		try {
			KeywordMatcherPerformanceTest.runTest("sample_queries.txt", 1);
			FuzzyTokenMatcherPerformanceTest.runTest("sample_query.txt", 1, Arrays.asList(0.8,0.65,0.5,0.35));
			

		} catch (StorageException | DataFlowException | IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
