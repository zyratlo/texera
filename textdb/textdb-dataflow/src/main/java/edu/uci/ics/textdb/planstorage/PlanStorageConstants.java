package edu.uci.ics.textdb.planstorage;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

/**
 * @author sweetest
 *
 */
public class PlanStorageConstants {
    public static final String PLAN_STORAGE_DIR = "../plan";

    public static final String PLAN_NAME_FIELD = "plan";
    public static final String PLAN_QUERY_FIELD = "query";

    public static final Attribute PLAN_NAME_ATTR = new Attribute(PLAN_NAME_FIELD, FieldType.STRING);
    public static final Attribute PLAN_QUERY_ATTR = new Attribute(PLAN_QUERY_FIELD, FieldType.STRING);

    public static final Attribute[] ATTRIBUTES_PLAN = {PLAN_NAME_ATTR, PLAN_QUERY_ATTR};
    public static final Schema SCHEMA_PLAN = new Schema(ATTRIBUTES_PLAN);
}
