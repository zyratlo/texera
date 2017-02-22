package edu.uci.ics.textdb.planstore;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * An implementation of query plan store.
 *
 * @author Adrian Seungjin Lee, Kishore Narendran
 */
public class PlanStore {
    private static PlanStore instance = null;
    private RelationManager relationManager = null;

    private PlanStore() throws StorageException, DataFlowException {
        relationManager = RelationManager.getRelationManager();
    }

    public synchronized static PlanStore getInstance() throws StorageException, DataFlowException {
        if (instance == null) {
            instance = new PlanStore();
        }
        return instance;
    }

    /**
     * Creates plan store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void createPlanStore() throws TextDBException {
        if (!relationManager.checkTableExistence(PlanStoreConstants.TABLE_NAME)) {
            relationManager.createTable(PlanStoreConstants.TABLE_NAME,
                    PlanStoreConstants.INDEX_DIR,
                    PlanStoreConstants.SCHEMA_PLAN,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }

        File filesDir = new File(PlanStoreConstants.FILES_DIR);
        if (!filesDir.exists()) {
            filesDir.mkdir();
        }
    }

    /**
     * removes plan store, both an index and a directory for plan objects.
     *
     * @throws TextDBException
     */
    public void destroyPlanStore() throws TextDBException {
        relationManager.deleteTable(PlanStoreConstants.TABLE_NAME);

        Utils.deleteDirectory(PlanStoreConstants.FILES_DIR);
    }

    /**
     * Adds a Logical Plan JSON to the plan store.
     *
     * @param planName, the name of the plan.
     * @param description, the description of the plan.
     * @param logicalPlanJson, the logical plan JSON string
     * @Return IDField, the id field of the plan stored.
     * @throws TextDBException, when there are null fields or the given name is invalid or there is an existing plan with same name.
     */
    public IDField addPlan(String planName, String description, String logicalPlanJson) throws TextDBException {
        if (planName == null || description == null || logicalPlanJson == null) {
            throw new TextDBException("Arguments cannot be null when adding a plan");
        }
        if (!PlanStoreConstants.VALID_PLAN_NAME.matcher(planName).find()) {
            throw new TextDBException("Plan name is not valid. It can only contain alphanumeric characters, " +
                    "underscore, and hyphen.");
        }
        if (getPlan(planName) != null) {
            throw new TextDBException("A plan with the same name already exists");
        }

        String filePath = PlanStoreConstants.FILES_DIR + "/" + planName + PlanStoreConstants.FILE_SUFFIX;
        ITuple tuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                new StringField(planName),
                new StringField(description),
                new StringField(filePath));

        DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);
        dataWriter.open();
        IDField id = dataWriter.insertTuple(tuple);
        dataWriter.close();
        
        writePlanJson(logicalPlanJson, filePath);

        return id;
    }

    /**
     * Retrieves a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @Return ITuple, the tuple consisting of fields of the plan.
     * @throws TextDBException
     */
    public ITuple getPlan(String planName) throws TextDBException {
        Query q = new TermQuery(new Term(PlanStoreConstants.NAME, planName));

        IDataReader reader = relationManager.getTableDataReader(PlanStoreConstants.TABLE_NAME, q);
        reader.open();

        ITuple inputTuple = null;

        while ((inputTuple = reader.getNextTuple()) != null) {
            IField nameField = inputTuple.getField(PlanStoreConstants.NAME);
            if (nameField.getValue().toString().equals(planName)) {
                reader.close();
                return inputTuple;
            }
        }

        reader.close();
        return null;
    }

    /**
     * Retrieves an iterator to scan through all plans.
     *
     * @Return IDataReader
     * @throws TextDBException
     */
    public IDataReader getPlanIterator() throws TextDBException {
        return relationManager.getTableDataReader(PlanStoreConstants.TABLE_NAME, new MatchAllDocsQuery());
    }

    /**
     * Removess a plan by given name from plan store.
     *
     * @param planName, the name of the plan.
     * @throws TextDBException
     */
    public void deletePlan(String planName) throws TextDBException {
        ITuple plan = getPlan(planName);

        if (plan == null) {
            return;
        }

        IDField idField = (IDField) plan.getField(SchemaConstants._ID);
        
        DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);        
        dataWriter.open();
        dataWriter.deleteTupleByID(idField);
        dataWriter.close();
        
        IField filePathField = plan.getField(PlanStoreConstants.FILE_PATH);
        deletePlanObject(filePathField.getValue().toString());
    }

    /**
     * Updates the description for the given plan name
     * @param planName - Name of the plan whose description is to be modified
     * @param description - New description of the plan as it it is to be updated
     * @throws TextDBException
     */
    public void updatePlanDescription(String planName, String description) throws TextDBException{
        updatePlanInternal(planName, description, null);
    }

    /**
     * Updates the logical plan for the given plan name
     * @param planName - Name of the plan which is to be modified
     * @param logicalPlanJson - New logical plan json as it is to be updated in the plan store
     * @throws TextDBException
     */
    public void updatePlan(String planName, String logicalPlanJson) throws TextDBException{
        updatePlanInternal(planName, null, logicalPlanJson);

    }

    /**
     * Updates both the description and the logical plan json for the given plan name
     * @param planName - Name of the plan which is to be updated
     * @param description - New description for the plan
     * @param logicalPlanJson - New logical plan json for the plan
     * @throws TextDBException
     */
    public void updatePlan(String planName, String description, String logicalPlanJson) throws TextDBException{
        updatePlanInternal(planName, description, logicalPlanJson);
    }

    /**
     * Updates both plan description and plan json of a plan with the given plan name.
     * If description is null, it will not update plan description.
     * If plan json is NULL, it will not update the plan's JSON file.
     *
     * @param planName, the name of the plan.
     * @param description, the new description of the plan.
     * @param logicalPlanJson, the new plan json string.
     * @throws TextDBException
     */
    private void updatePlanInternal(String planName, String description, String logicalPlanJson) throws TextDBException{
        ITuple existingPlan = getPlan(planName);

        if (existingPlan == null) {
            return;
        }

        if (description != null) {
            IDField idField = (IDField) existingPlan.getField(SchemaConstants._ID);
            IField descriptionField = new StringField(description);
            ITuple newTuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                    new StringField(planName),
                    descriptionField,
                    existingPlan.getField(PlanStoreConstants.FILE_PATH));
            DataWriter dataWriter = relationManager.getTableDataWriter(PlanStoreConstants.TABLE_NAME);
            dataWriter.open();
            dataWriter.updateTuple(newTuple, idField);
            dataWriter.close();
        }

        if (logicalPlanJson != null) {
            IField filePathField = existingPlan.getField(PlanStoreConstants.FILE_PATH);
            String filePath = filePathField.getValue().toString();
            deletePlanObject(filePath);
            writePlanJson(logicalPlanJson, filePath);
        }
    }

    /**
     * Retrieves the Logical Plan in JSON format at the given file path
     * @param filePath, the file path of the logical plan JSON
     * @return String - The logical plan JSON in String format
     * @throws TextDBException
     */
    public String readPlanJson(String filePath) throws TextDBException {
        try {
            // Reading the Logical JSON string as bytes
            byte[] encoded = Files.readAllBytes(Paths.get(filePath));
            return new String(encoded);
        }
        catch (IOException e) {
            throw new TextDBException("Failed to read the Logical Plan's JSON");
        }
    }

    /**
     * Writes the given Logical Plan JSON to a .json file, at the given file path
     * @param logicalPlanJson, the logical plan JSON in String format
     * @param filePath, the file path of the logical plan JSON file
     * @throws TextDBException
     */
    private void writePlanJson(String logicalPlanJson, String filePath) throws TextDBException {
        try {
            // Converting the JSON String to a JSON Node to minimize space usage and to check validity of JSON string
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(logicalPlanJson).getAsJsonObject();

            // Writing to the logical JSON string to a file
            FileWriter fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonObject.toString());
            fileWriter.flush();
            fileWriter.close();
        }
        catch (JsonParseException e) {
            throw new TextDBException("Logical Plan JSON is invalid", e);
        }
        catch (IOException e) {
            throw new TextDBException("Failed to write the Logical Plan JSON", e);
        }
    }

    /**
     * Removes a logical plan JSON file stored in the given file path.
     *
     * @param filePath, the file path of the plan object.
     * @throws TextDBException
     */
    private void deletePlanObject(String filePath) {
        File planFile = new File(filePath);
        if (planFile.exists()) {
            planFile.delete();
        }
    }
}
