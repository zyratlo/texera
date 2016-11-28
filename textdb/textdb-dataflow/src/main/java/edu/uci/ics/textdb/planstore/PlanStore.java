package edu.uci.ics.textdb.planstore;

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
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.storage.relation.RelationManager;

import java.io.*;

/**
 * Created by sweetest.sj on 11/13/16.
 */
public class PlanStore {
    private static PlanStore instance = null;
    private RelationManager im = null;

    private PlanStore() throws StorageException, DataFlowException {
        im = RelationManager.getRelationManager();
        //initialize
    }

    public synchronized static PlanStore getInstance() throws StorageException, DataFlowException {
        if (instance == null) {
            instance = new PlanStore();
        }
        return instance;
    }

    public void createPlanStore() throws TextDBException {
        if (!im.checkTableExistence(PlanStoreConstants.TABLE_NAME)) {
            im.createTable(PlanStoreConstants.TABLE_NAME,
                    PlanStoreConstants.INDEX_DIR,
                    PlanStoreConstants.SCHEMA_PLAN,
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }

        File filesDir = new File(PlanStoreConstants.FILES_DIR);
        if (!filesDir.exists()) {
            filesDir.mkdir();
        }
    }

    public void destroyPlanStore() throws TextDBException {
        im.deleteTable(PlanStoreConstants.TABLE_NAME);

        Utils.deleteIndex(PlanStoreConstants.FILES_DIR);
    }

    public IDField addPlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        if (planName == null || description == null || plan == null) {
            throw new TextDBException("arguments cannot be null when adding a plan");
        }
        if (!PlanStoreConstants.INVALID_PLAN_NAME.matcher(planName).find()) {
            throw new TextDBException("plan name is not valid, it can only contain alphanumeric characters, underscore, and hypen.");
        }
        if (getPlan(planName) != null) {
            throw new TextDBException("a plan with the same name already exists");
        }

        String filePath = PlanStoreConstants.FILES_DIR + "/" + planName + PlanStoreConstants.FILE_SUFFIX;
        ITuple tuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                new StringField(planName),
                new StringField(description),
                new StringField(filePath));

        IDField id = im.insertTuple(PlanStoreConstants.TABLE_NAME, tuple);
        writePlanObject(plan, filePath);

        return id;
    }

    public ITuple getPlan(String planName) throws TextDBException {
        IDataReader reader = im.scanTable(PlanStoreConstants.TABLE_NAME);
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

    public IDataReader getPlanIterator() throws TextDBException {
        return im.scanTable(PlanStoreConstants.TABLE_NAME);
    }

    public void deletePlan(String planName) throws TextDBException {
        ITuple plan = getPlan(planName);

        if (plan == null) {
            throw new TextDBException("plan with given name does not exist");
        }

        IField idField = plan.getField(SchemaConstants._ID);
        im.deleteTuple(PlanStoreConstants.TABLE_NAME, idField);

        IField filePathField = plan.getField(PlanStoreConstants.FILE_PATH);
        deletePlanObject(filePathField.getValue().toString());
    }

    public void updatePlan(String planName, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, null, plan);
    }

    public void updatePlan(String planName, String description) throws TextDBException {
        updatePlanInternal(planName, description, null);
    }

    public void updatePlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, description, plan);
    }

    private void updatePlanInternal(String planName, String description, LogicalPlan plan) throws TextDBException {
        ITuple tuple = getPlan(planName);

        if (tuple == null) {
            throw new TextDBException("plan with given name does not exist");
        }

        if (description != null) {
            IField idField = tuple.getField(SchemaConstants._ID);
            IField descriptionField = new StringField(description);
            ITuple newTuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                    new StringField(planName),
                    descriptionField,
                    tuple.getField(PlanStoreConstants.FILE_PATH));
            im.updateTuple(PlanStoreConstants.TABLE_NAME, newTuple, (IDField) idField);
        }

        if (plan != null) {
            IField filePathField = tuple.getField(PlanStoreConstants.FILE_PATH);
            String filePath = filePathField.getValue().toString();
            deletePlanObject(filePath);
            writePlanObject(plan, filePath);
        }
    }

    public LogicalPlan readPlanObject(String filePath) throws TextDBException {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(filePath);
            ois = new ObjectInputStream(fis);
            return (LogicalPlan) ois.readObject();
        } catch (Exception e) {
            throw new TextDBException("failed to read plan object", e);
        } finally {
            try {
                ois.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writePlanObject(LogicalPlan plan, String filePath) throws TextDBException {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            fos = new FileOutputStream(filePath);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(plan);
        } catch (IOException e) {
            throw new TextDBException("failed to write plan object", e);
        } finally {
            try {
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void deletePlanObject(String filePath) {
        File planFile = new File(filePath);
        if (planFile.exists()) {
            planFile.delete();
        }
    }
}
