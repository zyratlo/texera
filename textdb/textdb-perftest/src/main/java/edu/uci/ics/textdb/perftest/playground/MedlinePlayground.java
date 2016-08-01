package edu.uci.ics.textdb.perftest.playground;

import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.storage.DataStore;

public class MedlinePlayground {
	
	public static void main(String[] args) throws Exception {
		buildIndex();
	}
	
	public static void buildIndex() throws Exception {
		DataStore dataStore = new DataStore("./index/testindex/abstract_1K/", MedlineIndexWriter.SCHEMA_MEDLINE);
		Plan indexPlan = MedlineIndexWriter.getMedlineIndexPlan("./all-data-files/abstract_1K.txt", dataStore, new StandardAnalyzer());
		
		Engine.getEngine().evaluate(indexPlan);
	}
	
	public static void extract() throws Exception {
		DataStore dataStore = new DataStore("./index/testindex/abstract_1K/", MedlineIndexWriter.SCHEMA_MEDLINE);

		KeywordPredicate keywordPredicate = new KeywordPredicate("medicine", dataStore, Arrays.asList(MedlineIndexWriter.ABSTRACT_ATTR), new StandardAnalyzer(), KeywordMatchingType.CONJUNCTION_INDEXBASED);
		
		
	}

}
