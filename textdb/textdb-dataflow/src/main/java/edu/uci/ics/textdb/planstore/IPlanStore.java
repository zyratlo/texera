package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.plangen.LogicalPlan;

import java.util.List;

/**
 * @author sweetest
 * Schema of plan store consists of three fields
 * - name : name of plan
 * - description : detailed description of plan
 * - fileName : name of the file where the corresponding logicalPlan is stored
 *
 */
public interface IPlanStore {

    void createPlanStore() throws TextDBException;

    void destroyPlanStore() throws TextDBException;

    void addPlan(String planName, String description, LogicalPlan plan) throws TextDBException;

    ITuple getPlan(String planName) throws TextDBException;

    IDataReader getPlanIterator() throws TextDBException;

    void deletePlan(String planName) throws TextDBException;

    void updatePlan(String planName, LogicalPlan plan) throws TextDBException;

    void updatePlan(String planName, String description) throws TextDBException;

    void updatePlan(String planName, String description, LogicalPlan plan) throws TextDBException;
}