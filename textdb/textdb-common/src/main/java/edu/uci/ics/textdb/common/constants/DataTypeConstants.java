package edu.uci.ics.textdb.common.constants;

import java.util.Arrays;
import java.util.List;

public class DataTypeConstants {
    
    // This class doesn't need to be initialized.
    private DataTypeConstants() {
    }
    
    /**
     * A list of all attribute types (field types) of TextDB.
     */
    public static final List<String> attributeTypeList = Arrays.asList(
            "Integer", 
            "Double", 
            "Date", 
            "String", 
            "Text");

}
