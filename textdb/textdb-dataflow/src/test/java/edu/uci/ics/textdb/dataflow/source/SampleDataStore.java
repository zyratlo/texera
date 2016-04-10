/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import static edu.uci.ics.textdb.dataflow.source.TestConstants.FIRST_NAME;
import static edu.uci.ics.textdb.dataflow.source.TestConstants.LAST_NAME;
import static edu.uci.ics.textdb.dataflow.source.TestConstants.SAMPLE_TUPLES;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.dataflow.common.DataTuple;
import edu.uci.ics.textdb.dataflow.constants.LuceneConstants;

/**
 * @author sandeepreddy602
 *
 */
public class SampleDataStore {
    public SampleDataStore() {

    }
    
    public void clearData() throws IOException{
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(LuceneConstants.INDEX_DIR));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, conf);
            indexWriter.deleteAll();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (indexWriter != null) {
                indexWriter.close();
            }
        }
        
    }
    
    public void storeData() throws Exception {
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(LuceneConstants.INDEX_DIR));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, conf);
            
            for (ITuple sampleTuple : SAMPLE_TUPLES) {

                Document document = getDocument(sampleTuple);
                indexWriter.addDocument(document);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (indexWriter != null) {
                indexWriter.close();
            }
        }
    }

    private Document getDocument(ITuple tuple) {
        DataTuple sampleTuple = (DataTuple) tuple;
        Document doc = new Document();

        IField fnField = sampleTuple.getField(FIRST_NAME);
        String fn = "";
        if (fnField != null && fnField instanceof StringField) {
            fn = ((StringField) fnField).getValue();
        }

        IField lnField = sampleTuple.getField(LAST_NAME);
        String ln = "";
        if (lnField != null && lnField instanceof StringField) {
            ln = ((StringField) lnField).getValue();
        }

        IndexableField fnLuceneField = new org.apache.lucene.document.StringField(
                FIRST_NAME, fn, Store.YES);
        IndexableField lnLuceneField = new org.apache.lucene.document.StringField(
                LAST_NAME, ln, Store.YES);
        doc.add(fnLuceneField);
        doc.add(lnLuceneField);
        return doc;
    }
}
