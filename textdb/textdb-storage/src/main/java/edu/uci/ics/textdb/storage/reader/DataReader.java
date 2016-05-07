/**
 * 
 */
package edu.uci.ics.textdb.storage.reader;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * @author sandeepreddy602
 *
 */
public class DataReader implements IDataReader{

    private int cursor = CLOSED;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    private DataReaderPredicate dataReaderPredicate;

    public DataReader(IPredicate dataReaderPredicate) {
        this.dataReaderPredicate = (DataReaderPredicate)dataReaderPredicate;
    }
    
    @Override
    public void open() throws DataFlowException {


        try {
            String dataDirectory = dataReaderPredicate.getDataStore().getDataDirectory();
            Directory directory = FSDirectory.open(Paths.get(dataDirectory));
            indexReader = DirectoryReader.open(directory);
                		
            indexSearcher = new IndexSearcher(indexReader);
            TopDocs topDocs = indexSearcher.search(dataReaderPredicate.getQuery(), Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = OPENED;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        if(cursor == CLOSED){
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if(cursor >= scoreDocs.length){
                return null;
            }
            Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
            
            List<IField> fields = new ArrayList<IField>();
            Schema schema = dataReaderPredicate.getDataStore().getSchema();
            for (Attribute  attr : schema.getAttributes()) {
                FieldType fieldType = attr.getFieldType();
                String fieldValue = document.get(attr.getFieldName());
                fields.add(Utils.getField(fieldType, fieldValue));
            }
            
            DataTuple dataTuple = new DataTuple(schema, fields.toArray(new IField[fields.size()]));
            return dataTuple;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
    }

    @Override
    public void close() throws Exception {
        cursor = CLOSED;
        if(indexReader != null){
            try {
                indexReader.close();
                indexReader = null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
        }
    }
    
}
