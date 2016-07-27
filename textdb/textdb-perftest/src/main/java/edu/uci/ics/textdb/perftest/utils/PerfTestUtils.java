package edu.uci.ics.textdb.perftest.utils;

/**
 *@author Hailey Pan
 *
 *performance test helper functions 
 **/
import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.storage.DataStore;

public class PerfTestUtils {
	
	public static final String fileFolder = "./data-files/";
	public static final String standardIndexFolder = "./index/standard/";
	public static final String trigramIndexFolder = "./index/trigram/";
	public static final String resultFolder = "./data-files/results/";
	public static final String dictFolder = "./data-files/queries/";
	
	/**
	 * 
	 * @param testResults, a list of doubles
	 * @return the average of the values in testResults
	 */
	public static double calculateAverage(List<Double> testResults){
		double totalTime = 0;
		for(Double result: testResults){
			totalTime += result;
		}
		
		return Double.parseDouble(String.format("%.4f", totalTime/testResults.size()));
	}
	
	/**
	 * @param testResults, a list of doubles
	 * @param average, the average of the values in testResults
	 * @return standard deviation of the values in testResults
	 */
	public static double calculateSTD(List<Double> testResults, Double average){
		double numerator =0;
		for(Double result: testResults){
			numerator += Math.pow(result-average,2);
		}
		
		return Double.parseDouble(String.format("%.4f", Math.sqrt(numerator/testResults.size())));
	}
	
	/**
	 * Writes all files in ./data-files/ into indices
	 * @throws Exception
	 */
	public static void writeIndices() throws Exception {
		File files = new File(fileFolder);
		for(File file: files.listFiles()){
			if (file.getName().startsWith(".")) {
				continue;
			}
			if (file.isDirectory()) {
				continue;
			}
			writeIndex(file.getName() , new StandardAnalyzer(),"standard");
		}
	
	}
	
	/**
	 * Writes all files in ./data-files/ into trigram indices
	 * @throws Exception
	 */
	public static void writeTrigramIndices() throws Exception {
		File files = new File(fileFolder);
		for(File file: files.listFiles()){
			if (file.getName().startsWith(".")) {
				continue;
			}
			if (file.isDirectory()) {
				continue;
			}
			writeIndex(file.getName() ,  DataConstants.getTrigramAnalyzer(), "trigram");
		}
	
	}

	/**
	 * Writes a data file into an index
	 * 
	 * @param fileName, data file
	 * @param luceneAnalyzer
	 * @param indexType, indicates the types of index, trigram or standard 
	 * @throws Exception
	 */
	public static void writeIndex(String fileName,   Analyzer luceneAnalyzer, String indexType)
			throws Exception {
		DataStore dataStore = null;
		if(indexType.equalsIgnoreCase("trigram")){
			dataStore = new DataStore(getTrigramIndexPath(fileName.replace(".txt", "")), MedlineIndexWriter.SCHEMA_MEDLINE);
		}
		else {
			dataStore = new DataStore(getIndexPath(fileName.replace(".txt", "")), MedlineIndexWriter.SCHEMA_MEDLINE);
		}
		
		Engine writeIndexEngine = Engine.getEngine();

		writeIndexEngine.evaluate(MedlineIndexWriter.getMedlineIndexPlan(fileFolder+fileName, dataStore, luceneAnalyzer));
	
	}
	
	/**
	 * Reads each line in a file into a list
	 * 
	 * @param filePath
	 * @return a list of string
	 * @throws FileNotFoundException
	 */
	public static ArrayList<String> readDict(String filePath) throws FileNotFoundException{
		ArrayList<String> dict = new ArrayList<String>();
    	Scanner scanner = new Scanner(new File(filePath));
    	while (scanner.hasNextLine()){
    		dict.add(scanner.nextLine().trim());
    	}
    	scanner.close();
    	return dict;
	}
	
	/**
	 * 
	 * @param indexName
	 * @return a path of an index in ./index/standard/
	 */
	public static String getIndexPath(String indexName) {
		 return  standardIndexFolder+indexName;	
	}
	
	/**
	 * 
	 * @param indexName
	 * @return a path of a trigram index in ./index/trigram/
	 */
	public static String getTrigramIndexPath(String indexName) {
		return trigramIndexFolder+indexName; 
	}
	
	/**
	 * 
	 * @param resultFileName
	 * @return a path of a result file in ./data-files/results/
	 */
	public static String getResultPath(String resultFileName) {
			return resultFolder+resultFileName;	 
	}
	
	/**
	 * 
	 * @param dictFileName
	 * @return a path of a data file in ./data-files/dictionaries/
	 */
	public static String getDictPath(String dictFileName) {
			return dictFolder+dictFileName ;	
		 
	}
	
	/**
	 * Formats a time to string
	 * @param time (the milliseconds since January 1, 1970, 00:00:00 GMT)
	 * @return string representation of the time
	 */
	public static String formatTime(long time) {
        Date date = new Date(time);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
        
        return sdf.format(date).toString();
	}
	
	
}
