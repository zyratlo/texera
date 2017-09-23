package edu.uci.ics.texera.dataflow.operatorstore;

import java.nio.file.Path;

import edu.uci.ics.texera.api.utils.Utils;

public class OperatorStoreConstants {
    
    public static final String TABLE_NAME = "operator-metadata";
    
    public static final Path INDEX_DIR = Utils.getTexeraHomePath().resolve("index").resolve("operator-metadata");

    
    
    


}
