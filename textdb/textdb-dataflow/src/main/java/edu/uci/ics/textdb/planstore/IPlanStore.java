package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.plangen.OperatorGraph;

import java.util.List;

/**
 * @author sweetest
 * Schema of plan storage consists of two fields
 * - name : name of plan
 * - description : detailed description of plan
 * - query : json format of plan
 *
 */
public interface IPlanStore {

    void create();

    void destroyPlanStorage();

    void addPlan(String name, String description, OperatorGraph plan);

    ITuple getPlan(String name);

    List<ITuple> getAllPlans();

    void deletePlan(String name);

    void updatePlan(String name, OperatorGraph plan);

    void updatePlan(String name, String description);

    void updatePlan(String name, String description, OperatorGraph plan);
}