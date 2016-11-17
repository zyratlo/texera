package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IRelationManager;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.plangen.LogicalPlan;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created by sweetest.sj on 11/13/16.
 */
public class PlanStore implements IPlanStore {
    private static PlanStore instance = null;
    private IRelationManager im = null;

    private PlanStore() {
        //initialize
    }

    public synchronized static PlanStore getInstance() {
        if (instance == null) {
            instance = new PlanStore();
        }
        return instance;
    }

    @Override
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

    @Override
    public void destroyPlanStore() throws TextDBException {
        im.deleteTable(PlanStoreConstants.TABLE_NAME);

        Path directory = Paths.get(PlanStoreConstants.FILES_DIR);
        if (!Files.exists(directory)) {
            return;
        }

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new TextDBException("failed to delete plan files dir", e);
        }
    }

    @Override
    public void addPlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        if (planName == null || description == null || plan == null) {
            throw new TextDBException("arguments cannot be null when adding a plan");
        }
        if (PlanStoreConstants.INVALID_PLAN_NAME.matcher(planName).find()) {
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

        im.insertTuple(PlanStoreConstants.TABLE_NAME, tuple);
        writePlanObject(plan, filePath);
    }

    @Override
    public ITuple getPlan(String planName) throws TextDBException {
        IDataReader reader = im.scanTable(PlanStoreConstants.TABLE_NAME);
        ITuple inputTuple = null;

        while ((inputTuple = reader.getNextTuple()) != null) {
            IField nameField = inputTuple.getField(PlanStoreConstants.NAME);
            if (nameField.getValue().toString().equals(planName)) {
                return inputTuple;
            }
        }
        return null;
    }

    @Override
    public IDataReader getPlanIterator() throws TextDBException {
        return im.scanTable(PlanStoreConstants.TABLE_NAME);
    }

    @Override
    public void deletePlan(String planName) throws TextDBException {
        IDataReader reader = im.scanTable(PlanStoreConstants.TABLE_NAME);
        ITuple inputTuple = null;

        while ((inputTuple = reader.getNextTuple()) != null) {
            IField nameField = inputTuple.getField(PlanStoreConstants.NAME);
            if (nameField.getValue().toString().equals(planName)) {
                IField idField = inputTuple.getField("_id");
                im.deleteTuple(PlanStoreConstants.TABLE_NAME, idField);

                IField filePathField = inputTuple.getField(PlanStoreConstants.FILE_PATH);
                deletePlanObject(filePathField.getValue().toString());

                break;
            }
        }
    }

    @Override
    public void updatePlan(String planName, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, null, plan);
    }

    @Override
    public void updatePlan(String planName, String description) throws TextDBException {
        updatePlanInternal(planName, description, null);
    }

    @Override
    public void updatePlan(String planName, String description, LogicalPlan plan) throws TextDBException {
        updatePlanInternal(planName, description, plan);
    }

    private void updatePlanInternal(String planName, String description, LogicalPlan plan) throws TextDBException {
        IDataReader reader = im.scanTable(PlanStoreConstants.TABLE_NAME);
        ITuple inputTuple = null;

        while ((inputTuple = reader.getNextTuple()) != null) {
            IField nameField = inputTuple.getField(PlanStoreConstants.NAME);
            if (nameField.getValue().toString().equals(planName)) {
                if (description != null) {
                    IField idField = inputTuple.getField("_id");
                    IField descriptionField = new StringField(description);
                    ITuple tuple = new DataTuple(PlanStoreConstants.SCHEMA_PLAN,
                            new StringField(planName),
                            descriptionField,
                            inputTuple.getField(PlanStoreConstants.FILE_PATH));
                    im.updateTuple(PlanStoreConstants.TABLE_NAME, tuple, idField);
                }

                if (plan != null) {
                    IField filePathField = inputTuple.getField(PlanStoreConstants.FILE_PATH);
                    String filePath = filePathField.getValue().toString();
                    deletePlanObject(filePath);
                    writePlanObject(plan, filePath);
                }

                break;
            }
        }
    }

    public LogicalPlan readPlanObject(String filePath) throws TextDBException {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(filePath);
            ois = new ObjectInputStream(fis);
            return (LogicalPlan) ois.readObject();
        } catch(Exception e) {
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
