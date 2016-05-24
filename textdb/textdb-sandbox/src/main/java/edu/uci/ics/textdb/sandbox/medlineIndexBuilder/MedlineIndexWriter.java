package edu.uci.ics.textdb.sandbox.medlineIndexBuilder;

import java.io.FileNotFoundException;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/*
 * This class provides a helper function
 * that can write Medline data to index.
 * 
 * @author Zuozhi Wang
 */
public class MedlineIndexWriter {
	
	/*
	 * Write medline records from "medlineFilePath"
	 * to index in "indexPath".
	 */
	public static DataStore writeMedlineToIndex(
			String medlineFilePath, String indexPath, Analyzer luceneAnalyzer) 
			throws FileNotFoundException, StorageException {
		return writeMedlineToIndex(medlineFilePath, indexPath, luceneAnalyzer, Integer.MAX_VALUE);
	}

	/*
	 * Write a maximum of "maxDocNumber" records
	 * from "medlineFilePath"
	 * to index in "indexPath".
	 */
	public static DataStore writeMedlineToIndex(
			String medlineFilePath, String indexPath, Analyzer luceneAnalyzer, int maxDocNumber) 
			throws FileNotFoundException, StorageException {

		DataStore dataStore = new DataStore(DataConstants.INDEX_DIR, MedlineData.SCHEMA_MEDLINE);
		DataWriter dataWriter = new DataWriter(dataStore, luceneAnalyzer);
		dataWriter.clearData();
		dataWriter.open();
		
		MedlineData.open(medlineFilePath);
		
		int counter = 0;
		ITuple tuple = null;
		while ((tuple = MedlineData.getNextMedlineTuple()) != null 
				&& counter < maxDocNumber) {
			dataWriter.writeTuple(tuple);
			counter++;
		}
		
		MedlineData.close();
		dataWriter.close();

		return dataStore;
	}
	
}
