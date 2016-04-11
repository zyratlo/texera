package edu.uci.ics.textdb.storage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.Attribute;
import edu.uci.ics.textdb.common.field.FieldType;

public class DataStore {
    
    private String indexDir;

    public DataStore(String indexDir) {
        this.indexDir = indexDir;
    }
    
    public void clearData() throws Exception{
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(indexDir));
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
    
    public void storeData(List<Attribute> schema, List<ITuple> tuples) throws Exception {
        IndexWriter indexWriter = null;
        try {
            Directory directory = FSDirectory.open(Paths
                    .get(indexDir));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, conf);
            
            for (ITuple sampleTuple : tuples) {

                Document document = getDocument(schema, sampleTuple);
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

    private Document getDocument(List<Attribute> schema, ITuple tuple) {
        List<IField> fields = tuple.getFields();
        Document doc = new Document();
        for (int count = 0; count < fields.size(); count++) {
            IField field = fields.get(count);
            Attribute attr = schema.get(count);
            FieldType fieldType = attr.getFieldType();
            IndexableField luceneField = null;
            switch(fieldType){
                case STRING:
                    luceneField = new StringField(
                            attr.getFieldName(), (String) field.getValue(), Store.YES);
                    break;
                case INTEGER:
                    luceneField = new IntField(
                            attr.getFieldName(), (Integer) field.getValue(), Store.YES);
                    break;
                case DOUBLE:
                    double value = (Double) field.getValue();
                    luceneField = new DoubleField(
                            attr.getFieldName(), value, Store.YES);
                    break;
                case DATE:
                    String dateString = DateTools.dateToString((Date) field.getValue(), Resolution.MILLISECOND);
                    luceneField = new StringField(attr.getFieldName(), dateString, Store.YES);
                    break;
            }
            doc.add(luceneField);
        }
        return doc;
    }

}
