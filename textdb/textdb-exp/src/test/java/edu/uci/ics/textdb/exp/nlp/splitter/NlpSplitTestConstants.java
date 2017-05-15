package edu.uci.ics.textdb.exp.nlp.splitter;

import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

public class NlpSplitTestConstants {
    
    public static String TEXT = "text";
    public static String OUTPUT_TYPE = "text";
    
    public static Attribute TEXT_ATTRIBUTE = new Attribute("text", AttributeType.TEXT);
    
    public static Schema SPLIT_SCHEMA = new Schema(TEXT_ATTRIBUTE);
    
    public static Tuple MULTI_SENTENCE_TUPLE = new Tuple(SPLIT_SCHEMA, 
            new TextField("« Ce que nous avons fait, depuis tant et tant de mois. » 1,3 milliard est de 1300.00 millions."));
            //Programming is so fun. TextDB uses Java. Bugs are always annoying.
            //« Ce que nous avons fait, depuis tant et tant de mois. » 1,3 milliard est de 1300.00 millions.
            //السيد واين سريع. يدير 2.5 كم في 5 دقائق. هل تستطيع؟ لا يمكنك.
            //El Sr. Wayne es rápido. Corre 2.5 kms en 5 minutos. ¿Puedes? No puedes.
            //TODO: Help from human speakers needed to test.
}
