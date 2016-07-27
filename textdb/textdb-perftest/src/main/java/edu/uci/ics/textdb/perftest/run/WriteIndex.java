package edu.uci.ics.textdb.perftest.run;

/**
 * @author Hailey Pan
 * */

import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;

/*
 * Run this class to write all necessary index for performance tests!
 * */
public class WriteIndex {
	public static void main(String[] args){
		  
		try {
			PerfTestUtils.writeIndices();
			PerfTestUtils.writeTrigramIndices();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
