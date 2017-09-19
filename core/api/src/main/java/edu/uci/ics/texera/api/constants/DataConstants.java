/**
 * 
 */
package edu.uci.ics.texera.api.constants;

/**
 * @author Zuozhi Wang (zuozhi)
 */
public class DataConstants {
    
    public static final String HOME_ENV_VAR = "TEXERA_HOME";
    public static final String HOME_FOLDER_NAME = "core";
    
    public enum TexeraProject {
        TEXERA_API("api"),
        TEXERA_DATAFLOW("dataflow"),
        TEXERA_PERFTEST("perftest"),
        TEXERA_SANDBOX("sandbox"),
        TEXERA_STORAGE("storage"),
        TEXERA_TEXTQL("textql"),
        TEXERA_WEB("web");
        
        private String projectName;
        
        TexeraProject(String projectName) {
            this.projectName = projectName;
        }
        
        public String getProjectName() {
            return this.projectName;
        }
    }

}
