package edu.uci.ics.textdb.planstorage;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.exception.PlanStorageException;
import edu.uci.ics.textdb.common.exception.StorageException;

import java.io.IOException;
import java.util.List;

/**
 * @author sweetest
 * Schema of plan storage consists of two fields
 * - name : name of plan
 * - query : json format of plan
 *
 */
public class PlanStorage {

    public PlanStorage() throws IOException {
    }

    public void createPlanStorage() throws StorageException {
    }

    public void destroyPlanStorage() throws IOException {
    }

    public void addPlan(String planName, String plan) throws PlanStorageException {
    }

    public ITuple getPlan(String planName) throws PlanStorageException {
        return null;
    }

    public List<ITuple> getAllPlans() throws PlanStorageException {
        return null;
    }

    public void deletePlan(String planName) throws PlanStorageException {
    }

    public void updatePlan(String planName, String plan) throws PlanStorageException {
    }
}