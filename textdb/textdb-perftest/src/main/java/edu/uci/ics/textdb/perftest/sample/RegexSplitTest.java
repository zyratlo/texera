package edu.uci.ics.textdb.perftest.sample;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.source.FileRegexSplitSourceOperator;

public class RegexSplitTest {
//    public static final String URL = "url";
//    public static final String TITLE = "title";
    public static final String CONTENT = "content";
    
//    public static final Attribute URL_ATTR = new Attribute(URL, FieldType.STRING);
//    public static final Attribute TITLE_ATTR = new Attribute(TITLE, FieldType.TEXT);
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);
    
    public static final Schema FILE_TUPLE_SCHEMA = new Schema( CONTENT_ATTR);
//            URL_ATTR, TITLE_ATTR, CONTENT_ATTR);
    
    public static final String CHINESE_DATA_PATH = 
            "./sample-data-files/ChnieseTextDocumentTest.txt";
    
    public static void main(String[] args) throws Exception {
        test1();
    }
    
    public static void test1() throws Exception {
        String splitRegex = "^.{0,3}、|^.{0,2}．|^（. {0,3}）|^（.{0,3}）";
        
        FileRegexSplitSourceOperator tuplesSrc = new FileRegexSplitSourceOperator(
                CHINESE_DATA_PATH, splitRegex, FILE_TUPLE_SCHEMA);
        
        tuplesSrc.open();
        List<ITuple> results = new ArrayList<>();
        ITuple tuple;
        int counter = 0;
        while ((tuple = tuplesSrc.getNextTuple()) != null && counter < 10) {
            results.add(tuple);
            counter++;
        }
        tuplesSrc.close();
        
        System.out.println(results.size());
        System.out.println(Utils.getTupleListString(results));
    }

}
