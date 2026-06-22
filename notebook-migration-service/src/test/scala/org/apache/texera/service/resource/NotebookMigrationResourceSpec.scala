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
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.Notebook.NOTEBOOK
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.Workflow.WORKFLOW
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING
import org.apache.texera.dao.jooq.generated.tables.WorkflowUserAccess.WORKFLOW_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.WorkflowVersion.WORKFLOW_VERSION
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowUserAccess,
  WorkflowVersion
}
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
  private val writerEmail = s"nbms_writer_$testWid@example.com"
  private val readerEmail = s"nbms_reader_$testWid@example.com"

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var userDao: UserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var seededVid: Integer = _
  private var writerUid: Integer = _ // holds WRITE access to testWid
  private var readerUid: Integer = _ // holds READ access to testWid

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
    userDao = new UserDao(cfg)
    workflowUserAccessDao = new WorkflowUserAccessDao(cfg)
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

    // One user with WRITE access (the happy path) and one with only READ
    // access, so the write-access gate can be exercised both ways.
    writerUid = insertUser("nbms_writer", writerEmail)
    readerUid = insertUser("nbms_reader", readerEmail)
    insertAccess(writerUid, PrivilegeEnum.WRITE)
    insertAccess(readerUid, PrivilegeEnum.READ)
  }

  override protected def afterEach(): Unit = cleanup()

  private def insertUser(name: String, email: String): Integer = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("password")
    userDao.insert(user)
    user.getUid
  }

  private def insertAccess(uid: Integer, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setWid(testWid)
    access.setUid(uid)
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }

  private def cleanup(): Unit = {
    // Delete children before parents. workflow_user_access and notebook cascade
    // on workflow delete, but explicit deletes keep state observable across tests
    // and avoid depending on cascade ordering.
    getDSLContext.deleteFrom(WORKFLOW_NOTEBOOK_MAPPING).execute()
    getDSLContext.deleteFrom(NOTEBOOK).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.EMAIL.in(writerEmail, readerEmail)).execute()
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
    val response = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
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

    NotebookMigrationResource.storeNotebookAndMapping(storePayload(notebook, mapping), writerUid)

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
      storePayload(vid = unknownVid),
      writerUid
    )
    response.getStatus shouldBe Response.Status.INTERNAL_SERVER_ERROR.getStatusCode
    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 0
  }

  it should "return 409 Conflict on a second store for a workflow that already has a notebook" in {
    // notebook.wid is UNIQUE — one notebook per workflow. The second store must be
    // rejected with an explicit 409 (not a 500 from the constraint violation), and
    // must not add a second notebook or mapping row.
    val first = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    first.getStatus shouldBe Response.Status.OK.getStatusCode

    val second = NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)
    second.getStatus shouldBe Response.Status.CONFLICT.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 1
    getDSLContext.fetchCount(WORKFLOW_NOTEBOOK_MAPPING) shouldBe 1
  }

  // -- fetchNotebookAndMapping ------------------------------------------------

  "fetchNotebookAndMapping" should "return exists=false when no notebook is stored for the (wid, vid)" in {
    val response = NotebookMigrationResource.fetchNotebookAndMapping(fetchPayload(), writerUid)
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity.toString should include("\"exists\": false")
  }

  it should "return exists=true with the stored notebook and mapping when a row exists" in {
    NotebookMigrationResource.storeNotebookAndMapping(storePayload(), writerUid)

    val entity =
      NotebookMigrationResource
        .fetchNotebookAndMapping(fetchPayload(), writerUid)
        .getEntity
        .toString
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

    NotebookMigrationResource.storeNotebookAndMapping(
      storePayload(notebook, sampleMapping),
      writerUid
    )

    val entity =
      NotebookMigrationResource
        .fetchNotebookAndMapping(fetchPayload(), writerUid)
        .getEntity
        .toString
    entity should include("\"v1\"")
  }

  // -- workflow write-access enforcement --------------------------------------

  "store/fetch" should "return 403 Forbidden when the user lacks write access to the workflow" in {
    // readerUid holds only READ access; the endpoints require WRITE, so both must
    // be rejected with a 403 and no notebook may be written.
    NotebookMigrationResource
      .storeNotebookAndMapping(storePayload(), readerUid)
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

    NotebookMigrationResource
      .fetchNotebookAndMapping(fetchPayload(), readerUid)
      .getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

    getDSLContext.fetchCount(NOTEBOOK) shouldBe 0
  }

  // -- setNotebook ------------------------------------------------------------

  "setNotebook" should "reject a notebook name that is not a plain .ipynb filename with 400" in {
    // The name is validated before any Jupyter call, so these are rejected with a
    // 400 without a running Jupyter server. Covers path traversal, a wrong
    // extension, and an embedded subpath.
    Seq("../../etc/evil.ipynb", "notebook.txt", "work/notebook.ipynb").foreach { name =>
      val body = s"""{"notebookName": "$name", "notebookData": {"cells": []}}"""
      withClue(s"name=$name: ") {
        NotebookMigrationResource
          .setNotebook(body)
          .getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
      }
    }
  }
}
