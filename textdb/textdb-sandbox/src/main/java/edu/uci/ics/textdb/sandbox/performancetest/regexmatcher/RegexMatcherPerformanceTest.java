package edu.uci.ics.textdb.sandbox.performancetest.regexmatcher;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.sandbox.medlineIndexBuilder.MedlineData;
import edu.uci.ics.textdb.sandbox.medlineIndexBuilder.MedlineIndexWriter;
import edu.uci.ics.textdb.storage.DataStore;

/*
 * This is a sample performance test 
 * using Medline data and helper functions.
 * 
 * @author Zuozhi Wang
 */
public class RegexMatcherPerformanceTest {
	
	public static void main(String[] args) throws StorageException, IOException, DataFlowException {
		performanceTest1();
	}

	public static void performanceTest1() throws StorageException, IOException, DataFlowException {
		
		Analyzer luceneAnalyzer = CustomAnalyzer.builder()
				.withTokenizer(NGramTokenizerFactory.class, new String[]{"minGramSize", "3", "maxGramSize", "3"})
				.build();
		
		long startIndexTime = System.currentTimeMillis();
		
		DataStore dataStore = MedlineIndexWriter.writeMedlineToIndex(
				"/Users/georgewang/Documents/Project_Medextract/dataset/abstract_100K.txt", "./index", luceneAnalyzer);
		
		long endIndexTime = System.currentTimeMillis();
		double indexTime = (endIndexTime - startIndexTime)/1000.0;
		System.out.printf("index time: %f\n", indexTime);
		
		
		String regex = "water";
		Attribute[] attributeList = new Attribute[]{ MedlineData.ABSTRACT_ATTR };
		
		RegexPredicate regexPredicate = new RegexPredicate(
				regex, Arrays.asList(attributeList), 
				luceneAnalyzer, dataStore);
		
		RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
		regexMatcher.open();
		
		long startMatchTime = System.currentTimeMillis();

		int counter = 0;
		ITuple nextTuple = null;
		while ((nextTuple = regexMatcher.getNextTuple()) != null) {
			counter++;
		}
		
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime)/1000.0;
		System.out.printf("match time: %f\n", matchTime);
		
		System.out.printf("total: %d results\n", counter);
	}
	
}
