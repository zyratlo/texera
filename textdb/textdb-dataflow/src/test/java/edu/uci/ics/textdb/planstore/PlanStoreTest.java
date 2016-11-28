package edu.uci.ics.textdb.planstore;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.plangen.LogicalPlan;
import edu.uci.ics.textdb.plangen.LogicalPlanTest;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author sweetest
 */
public class PlanStoreTest {
    private PlanStore planStore;

    @Before
    public void setUp() throws Exception {
        planStore = PlanStore.getInstance();
        planStore.createPlanStore();
    }

    @After
    public void cleanUp() throws Exception {
        planStore.destroyPlanStore();
    }

    @Test
    public void testPlanStore() throws TextDBException {
        LogicalPlan logicalPlan1 = LogicalPlanTest.getLogicalPlan1();
        LogicalPlan logicalPlan2 = LogicalPlanTest.getLogicalPlan2();
        LogicalPlan logicalPlan3 = LogicalPlanTest.getLogicalPlan3();

        String planName1 = "plan1";
        String planName2 = "plan2";

        planStore.addPlan(planName1, "basic regex plan", logicalPlan1);

        ITuple res1 = planStore.getPlan(planName1);

        Assert.assertNotNull(res1);

        String filePath = res1.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
        LogicalPlan returnedPlan1 = planStore.readPlanObject(filePath);

        Assert.assertEquals(logicalPlan1, returnedPlan1);

        planStore.updatePlan(planName1, logicalPlan2);

        ITuple res2 = planStore.getPlan(planName1);

        Assert.assertNotNull(res2);
        filePath = res2.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
        LogicalPlan returnedPlan2 = planStore.readPlanObject(filePath);

        Assert.assertNotSame(logicalPlan1, returnedPlan2);
        Assert.assertEquals(logicalPlan2, returnedPlan2);

        planStore.addPlan(planName2, "more complex plan", logicalPlan3);

        IDataReader reader = planStore.getPlanIterator();
        reader.open();

        ITuple tuple = null;
        List<LogicalPlan> returnedPlans = new ArrayList<>();
        List<LogicalPlan> expectedPlans = new ArrayList<>();
        expectedPlans.add(logicalPlan2);
        expectedPlans.add(logicalPlan3);

        while ((tuple = reader.getNextTuple()) != null) {
            filePath = tuple.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
            LogicalPlan logicalPlan = planStore.readPlanObject(filePath);
            returnedPlans.add(logicalPlan);
        }
        reader.close();

        Assert.assertEquals(expectedPlans, returnedPlans);

        planStore.deletePlan(planName1);

        ITuple plan2 = planStore.getPlan(planName1);
        Assert.assertNull(plan2);
    }

    @Test(expected = TextDBException.class)
    public void testAddPlanWithInvalidName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan/regex";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test(expected = TextDBException.class)
    public void testAddPlanWithEmptyName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test(expected = TextDBException.class)
    public void testAddMultiplePlansWithSameName() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);
        planStore.addPlan(planName, "basic regex plan", logicalPlan);
    }

    @Test(expected = TextDBException.class)
    public void testDeleteNotExistingPlan() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        planStore.deletePlan(planName + planName);
    }

    @Test(expected = TextDBException.class)
    public void testUpdateNotExistingPlan() throws TextDBException {
        LogicalPlan logicalPlan = LogicalPlanTest.getLogicalPlan1();

        String planName = "plan";

        planStore.addPlan(planName, "basic regex plan", logicalPlan);

        planStore.updatePlan(planName+planName, "basic regex plan", logicalPlan);
    }
}
