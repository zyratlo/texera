package edu.uci.ics.texera.exp.resource.dictionary;

import java.nio.file.Path;
import java.nio.file.Paths;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.utils.Utils;

public class DictionaryManagerConstants {

    public static final String TABLE_NAME = "dictionary";

    public static final String INDEX_DIR = Paths.get(Utils.getTexeraHomePath(), "index/dictionary").toString();
    
    public static final Path DICTIONARY_DIR_PATH = Paths.get(Utils.getTexeraHomePath(), "user-resources", "dictionary");
    public static final String DICTIONARY_DIR = DICTIONARY_DIR_PATH.toString();

    public static final String NAME = "name";
    public static final Attribute NAME_ATTR = new Attribute(NAME, AttributeType.STRING);
    
    public static final Schema SCHEMA = new Schema(NAME_ATTR);
    
}
