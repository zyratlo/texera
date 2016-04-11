package edu.uci.ics.textdb.storage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.Attribute;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.FieldType;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

public class SampleScanBasedOperator implements IOperator{

    
    private String dataDir;
    private List<Attribute> schema;
    private int cursor = -1;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    
    public SampleScanBasedOperator(String dataDirectory, List<Attribute> schema) {
        this.dataDir = dataDirectory;
        this.schema = schema;
    }

    @Override
    public void open() throws DataFlowException {

        try {
            Directory directory = FSDirectory.open(Paths.get(dataDir));
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            Analyzer analyzer = new StandardAnalyzer();
            Attribute defaultAttr = schema.get(0);
            QueryParser queryParser = new QueryParser(defaultAttr.getFieldName(), analyzer);
            Query query = queryParser.parse(LuceneConstants.SCAN_QUERY);
            
            TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = OPENED;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        } catch (ParseException e) {
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
            for (Attribute  attr : schema) {
                FieldType fieldType = attr.getFieldType();
                IField field = null;
                String fieldValue = document.get(attr.getFieldName());
                switch (fieldType) {
                    case STRING:
                        field = new StringField(fieldValue);
                        break;
                    case INTEGER:
                        field = new IntegerField(Integer.parseInt(fieldValue));
                        break;
                    case DOUBLE:
                        field = new DoubleField(Double.parseDouble(fieldValue));
                        break;
                    case DATE:
                        field = new DateField(DateTools.stringToDate(fieldValue));
                        break;
                    default:
                        break;
                }
                
                fields.add(field);
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
    public void close() throws DataFlowException {
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
