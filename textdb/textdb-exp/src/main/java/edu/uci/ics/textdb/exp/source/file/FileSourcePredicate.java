package edu.uci.ics.textdb.exp.source.file;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class FileSourcePredicate {
    
    private final String filePath;
    private final String attributeName;
    
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
            String filePath, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attributeName) {
        this.filePath = filePath;
        this.attributeName = attributeName;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getFilePath() {
        return this.filePath;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return this.attributeName;
    }

}
