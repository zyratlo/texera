package edu.uci.ics.textdb.perftest.dictionarymatcher;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants.DictionaryOperatorType;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.medline.MedlineReader;
import edu.uci.ics.textdb.storage.DataStore;

public class DictionaryPerformanceTest {
	public static DataStore dataStore;
	public static StandardAnalyzer luceneAnalyzer;
	public static ArrayList<String> dict = new ArrayList<String>();
	
	public static final String fileFolder = "./data-files/";
	public static final String indexFolder = "./index/";
	
	public static void main(String[] args) throws Exception{
		samplePerformanceTest("abstract_10K", "./index");
	}

	public static void writeIndex(String fileName, Analyzer luceneAnalyzer) throws FileNotFoundException, StorageException{
		long startIndexTime = System.currentTimeMillis(); 
		
		dataStore = new DataStore(indexFolder+"dict/"+fileName, MedlineReader.SCHEMA_MEDLINE);
		MedlineIndexWriter.writeMedlineToIndex(fileFolder+fileName+".txt", dataStore, luceneAnalyzer);
		long endIndexTime = System.currentTimeMillis();
		double indexTime = (endIndexTime - startIndexTime)/1000.0;
		System.out.printf("index time: %.4f seconds\n", indexTime);
	}
	
	public static void loadIndex(String fileName, Analyzer luceneAnalyzer) throws FileNotFoundException, StorageException{ 
		dataStore = new DataStore(indexFolder+"dict/"+fileName, MedlineReader.SCHEMA_MEDLINE);
	}
	
	public static void samplePerformanceTest(String filePath, String indexPath) 
			throws Exception {
		Scanner scanner = new Scanner( System.in );
		luceneAnalyzer = new StandardAnalyzer();
		while (true){
			System.out.println("================================");
			System.out.println("q --- quit");
			System.out.println("w --- write index");
			System.out.println("l --- load index");
			System.out.println("m --- start query and match");
			System.out.println("================================");
			String command = scanner.nextLine();
			if (command.equals("w")){
				writeIndex(filePath, luceneAnalyzer);
			}
			if (command.equals("l")){
				loadIndex(filePath, luceneAnalyzer);
			}
			if (command.equals("m")){
				match();
			}
			if (command.equals("q")){
				break;
			}
		}
	}
	
	public static void readDict(String filePath) throws FileNotFoundException{
    	Scanner scanner = new Scanner(new File(filePath));
    	while (scanner.hasNextLine()){
    		dict.add(scanner.nextLine().trim());
    	}
    	scanner.close();
	}
	

	public static void match() throws Exception{
		List<Attribute> attributes = Arrays.asList(MedlineReader.ABSTRACT_ATTR);
		ArrayList<String> queryList = new ArrayList<String>();
		
		//dict.add("medical");
		readDict("/Users/Shirley/Desktop/dictionaries/WebMD_symptoms.txt");
		
		for (String d:dict){
			System.out.print("\n"+d+"\n");
			IDictionary dictionary = new Dictionary(dict);
	    	IPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, luceneAnalyzer, attributes, DictionaryOperatorType.SCAN, dataStore);
	    	DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);
	    	long startLuceneQueryTime = System.currentTimeMillis();
	    	dictionaryMatcher.open();
	    	long endLuceneQueryTime = System.currentTimeMillis();
			double luceneQueryTime = (endLuceneQueryTime - startLuceneQueryTime)/1000.0;
			System.out.printf("lucene Query time: %.4f seconds\n", luceneQueryTime);
			
			long startMatchTime = System.currentTimeMillis();
	        ITuple nextTuple = null;
	        int counter = 0;
	        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
	            counter++;
	        }
	        dictionaryMatcher.close();
	        
			long endMatchTime = System.currentTimeMillis();
			double matchTime = (endMatchTime - startMatchTime)/1000.0;
			System.out.printf("match time: %.4f seconds\n", matchTime);		
			System.out.printf("total: %d results\n", counter);
		}
		
	}
}