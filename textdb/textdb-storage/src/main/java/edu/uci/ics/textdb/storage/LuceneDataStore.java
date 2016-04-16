package edu.uci.ics.textdb.storage;

public class LuceneDataStore {
    public static final String DATA_STORE_DIRECTORY = "../index";
    private static int numDocuments = 0;
    
    public static void incrementNumDocuments(int incrementBy) {
        numDocuments += incrementBy;
    }
    
    public static int getNumDocuments() {
        return numDocuments;
    }
    
}
