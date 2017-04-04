package edu.uci.ics.textdb.exp.source;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class FileSourcePredicate {
    
    private final String filePath;
    
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
            String filePath) {
        this.filePath = filePath;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getFilePath() {
        return this.filePath;
    }

}
