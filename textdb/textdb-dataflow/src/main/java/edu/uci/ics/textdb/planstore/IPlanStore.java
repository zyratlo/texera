package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.ITuple;
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

    void createPlanStore();

    void destroyPlanStore();

    void addPlan(String planName, String description, LogicalPlan plan);

    ITuple getPlan(String planName);

    List<ITuple> getAllPlans();

    void deletePlan(String planName);

    void updatePlan(String planName, LogicalPlan plan);

    void updatePlan(String planName, String description);

    void updatePlan(String planName, String description, LogicalPlan plan);
}