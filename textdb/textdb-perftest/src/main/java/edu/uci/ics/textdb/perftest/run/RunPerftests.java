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

public class RunPerftests {

	/**
	 * Run all performance tests 
	 * make necessary changes for arguments
	 *
	 * */
	public static void main(String[] args){
		try {
			KeywordMatcherPerformanceTest.runTest("sample_queries.txt", 1);
			 
			
		} catch (StorageException | DataFlowException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
