/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
SET search_path TO texera_db;

-- ============================================
-- 2. Delete tables if they already exist
-- ============================================

BEGIN;

DROP TABLE IF EXISTS notebook CASCADE;
DROP TABLE IF EXISTS workflow_notebook_mapping CASCADE;

-- ============================================
-- 3. Create the tables to store notebook and mapping
-- ============================================

CREATE TABLE notebook (
    nid         SERIAL  NOT NULL PRIMARY KEY,
    wid         INT     NOT NULL,
    notebook    JSONB   NOT NULL,
    FOREIGN KEY (wid) REFERENCES workflow(wid) ON DELETE CASCADE
);

CREATE TABLE workflow_notebook_mapping (
    wid         INT     NOT NULL,
    vid         INT     NOT NULL,
    nid         INT     NOT NULL,
    mapping     JSONB   NOT NULL,
    PRIMARY KEY (wid, vid, nid),
    FOREIGN KEY (wid) REFERENCES workflow(wid) ON DELETE CASCADE,
    FOREIGN KEY (vid) REFERENCES workflow_version(vid) ON DELETE CASCADE,
    FOREIGN KEY (nid) REFERENCES notebook(nid) ON DELETE CASCADE
);

COMMIT;
