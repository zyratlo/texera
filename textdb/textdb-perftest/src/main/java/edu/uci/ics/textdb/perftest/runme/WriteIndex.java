package edu.uci.ics.textdb.perftest.runme;

/**
 * @author Hailey Pan
 * */

import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;

/*
 * Run this class to write all necessary index for performance tests!
 * */
public class WriteIndex {
	public static void main(String[] args) {
		if(args.length !=0){
			PerfTestUtils.setFileFolder(args[0]);
			PerfTestUtils.setStandardIndexFolder(args[1]);
			PerfTestUtils.setTrigramIndexFolder(args[2]);
		}

		try {
			PerfTestUtils.writeStandardAnalyzerIndices();
			PerfTestUtils.writeTrigramIndices();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
