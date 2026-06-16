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

package org.apache.texera.service.resource

import jakarta.ws.rs.core.Response
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.tables.Notebook.NOTEBOOK
import org.apache.texera.dao.jooq.generated.tables.Workflow.WORKFLOW
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING
import org.apache.texera.dao.jooq.generated.tables.WorkflowVersion.WORKFLOW_VERSION
import org.apache.texera.dao.jooq.generated.tables.daos.{WorkflowDao, WorkflowVersionDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Workflow, WorkflowVersion}
import org.jooq.JSONB
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.util.UUID

class NotebookMigrationResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Randomise the seeded wid so a parallel run of unrelated specs that happen
  // to seed the same id wouldn't collide on the embedded postgres.
  private val testWid = 9000 + scala.util.Random.nextInt(1000)

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var seededVid: Integer = _

  private val sampleNotebook =
    """{"cells":[{"cell_type":"code","metadata":{},"source":"print(1)"}]}"""
  private val sampleMapping =
    """{"operator_to_cell":{},"cell_to_operator":{}}"""

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()
  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    val cfg = getDSLContext.configuration()
    workflowDao = new WorkflowDao(cfg)
    workflowVersionDao = new WorkflowVersionDao(cfg)
    cleanup()

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"wf_${UUID.randomUUID().toString.substring(0, 8)}")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(version)
    seededVid = version.getVid
  }

  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit = {
    // notebook and workflow_notebook_mapping cascade on workflow/version delete,
    // but explicit deletes here keep state observable across tests and avoid
    // depending on cascade ordering.
    getDSLContext.deleteFrom(WORKFLOW_NOTEBOOK_MAPPING).execute()
    getDSLContext.deleteFrom(NOTEBOOK).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
  }

  private def storePayload(
      notebook: String = sampleNotebook,
      mapping: String = sampleMapping,
      vid: Integer = seededVid
  ): String =
    s"""{"wid": $testWid, "vid": $vid, "notebook": $notebook, "mapping": $mapping}"""

  private def fetchPayload(vid: Integer = seededVid): String =
    s"""{"wid": $testWid, "vid": $vid}"""

  // -- storeNotebookAndMapping ------------------------------------------------

  "storeNotebookAndMapping" should "insert one notebook and one mapping tied to the workflow version" in {
    val response = NotebookMigrationResource.storeNotebookAndMapping(storePayload())
    response.getStatus shouldBe Response.Status.OK.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1

    val notebookRow = getDSLContext.selectFrom(NOTEBOOK).fetchOne()
    notebookRow.get(NOTEBOOK.WID) shouldBe testWid

    val mappingRow = getDSLContext.selectFrom(WORKFLOW_NOTEBOOK_MAPPING).fetchOne()
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.WID) shouldBe testWid
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.VID) shouldBe seededVid
    // The mapping row must reference the just-inserted notebook by its returned nid.
    mappingRow.get(WORKFLOW_NOTEBOOK_MAPPING.NID) shouldBe notebookRow.get(NOTEBOOK.NID)
  }

  it should "round-trip notebook and mapping JSON content through the JSONB columns" in {
    val notebook =
      """{"cells":[{"cell_type":"code","metadata":{"uuid":"abc-123"},"source":"x = 1"}]}"""
    val mapping =
      """{"operator_to_cell":{"op1":["cell1"]},"cell_to_operator":{"cell1":["op1"]}}"""

    NotebookMigrationResource.storeNotebookAndMapping(storePayload(notebook, mapping))

    val storedNotebookJson =
      getDSLContext
        .selectFrom(NOTEBOOK)
        .fetchOne()
        .get(NOTEBOOK.NOTEBOOK_)
        .asInstanceOf[JSONB]
        .data()
    val storedMappingJson =
      getDSLContext
        .selectFrom(WORKFLOW_NOTEBOOK_MAPPING)
        .fetchOne()
        .get(WORKFLOW_NOTEBOOK_MAPPING.MAPPING)
        .asInstanceOf[JSONB]
        .data()

    // Use whitespace-agnostic substring checks — postgres canonicalises JSONB
    // text on the way out, so exact-string compare against the input would be
    // fragile across postgres versions.
    storedNotebookJson should include("\"abc-123\"")
    storedNotebookJson should include("x = 1")
    storedMappingJson should include("\"op1\"")
    storedMappingJson should include("\"cell1\"")
  }

  it should "roll back the notebook insert when the mapping insert fails its FK constraint" in {
    // workflow_notebook_mapping.vid has FK -> workflow_version(vid). Passing an
    // unknown vid trips the mapping insert; because both inserts share a single
    // SqlServer.withTransaction block, the notebook insert must roll back too.
    // Without this guarantee, orphaned notebook rows would accumulate on every
    // failed store.
    val unknownVid: Integer = -1
    val response = NotebookMigrationResource.storeNotebookAndMapping(
      storePayload(vid = unknownVid)
    )
    response.getStatus shouldBe Response.Status.INTERNAL_SERVER_ERROR.getStatusCode
    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 0
  }

  // -- fetchNotebookAndMapping ------------------------------------------------

  "fetchNotebookAndMapping" should "return exists=false when no notebook is stored for the (wid, vid)" in {
    val response = NotebookMigrationResource.fetchNotebookAndMapping(fetchPayload())
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity.toString should include("\"exists\": false")
  }

  it should "return exists=true with the stored notebook and mapping when a row exists" in {
    NotebookMigrationResource.storeNotebookAndMapping(storePayload())

    val entity =
      NotebookMigrationResource.fetchNotebookAndMapping(fetchPayload()).getEntity.toString
    entity should include("\"exists\": true")
    entity should include("\"notebook\":")
    entity should include("\"mapping\":")
  }

  it should "return the stored notebook content for a (wid, vid) on fetch" in {
    // notebook.wid is UNIQUE — one notebook per workflow — so the endpoint's
    // orderBy(NID.desc).limit(1) resolves to that single row. This pins the
    // workflow-reopen path: after a store, fetch must return that notebook's content.
    val notebook =
      """{"cells":[{"cell_type":"code","metadata":{},"source":"v1"}]}"""

    NotebookMigrationResource.storeNotebookAndMapping(storePayload(notebook, sampleMapping))

    val entity =
      NotebookMigrationResource.fetchNotebookAndMapping(fetchPayload()).getEntity.toString
    entity should include("\"v1\"")
  }
}
