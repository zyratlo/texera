package edu.uci.ics.textdb.sandbox.medlineIndexBuilder;

import java.io.FileNotFoundException;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class MedlineIndexWriter {

	public static DataStore writeMedlineToIndex(
			String medlineFilePath, String indexPath, Analyzer luceneAnalyzer, int docNumber) 
			throws FileNotFoundException, StorageException {

		DataStore dataStore = new DataStore(DataConstants.INDEX_DIR, MedlineData.SCHEMA_MEDLINE);
		DataWriter dataWriter = new DataWriter(dataStore, luceneAnalyzer);
		dataWriter.clearData();
		dataWriter.open();
		
		MedlineData.open(medlineFilePath);
		
		int counter = 0;
		ITuple tuple = MedlineData.getNextMedlineTuple();
		while (tuple != null && counter < docNumber) {
			dataWriter.writeTuple(tuple);
			
			tuple = MedlineData.getNextMedlineTuple();
			counter++;
		}
		
		MedlineData.close();
		dataWriter.close();

		return dataStore;
	}
	
}
