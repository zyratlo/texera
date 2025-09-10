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

package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowOfProjectDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos._
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.web.resource.dashboard.hub.EntityType
import edu.uci.ics.texera.web.resource.dashboard.hub.HubResource.recordCloneAction
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource.hasReadAccess
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource._
import io.dropwizard.auth.Auth
import org.jooq.impl.DSL.{groupConcatDistinct, noCondition}
import org.jooq.{Condition, Record9, Result, SelectOnConditionStep}

import java.sql.Timestamp
import java.util
import java.util.UUID
import javax.annotation.security.RolesAllowed
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
  * This file handles various request related to saved-workflows.
  * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
  * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
  */

object WorkflowResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val workflowDao = new WorkflowDao(context.configuration)
  final private lazy val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  final private lazy val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration()
  )
  final private lazy val workflowOfProjectDao = new WorkflowOfProjectDao(context.configuration)

  def getWorkflowName(wid: Integer): String = {
    val workflow = workflowDao.fetchOneByWid(wid)
    if (workflow == null) {
      throw new NotFoundException(s"Workflow with id $wid not found")
    }
    workflow.getName
  }

  private def insertWorkflow(workflow: Workflow, user: User): Unit = {
    workflowDao.insert(workflow)
    workflowOfUserDao.insert(new WorkflowOfUser(user.getUid, workflow.getWid))
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(
        user.getUid,
        workflow.getWid,
        PrivilegeEnum.WRITE
      )
    )
  }

  private def workflowOfUserExists(wid: Integer, uid: Integer): Boolean = {
    workflowOfUserDao.existsById(
      context
        .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
        .values(uid, wid)
    )
  }

  private def workflowOfProjectExists(wid: Integer, pid: Integer): Boolean = {
    workflowOfProjectDao.existsById(
      context
        .newRecord(WORKFLOW_OF_PROJECT.WID, WORKFLOW_OF_PROJECT.PID)
        .values(wid, pid)
    )
  }

  case class DashboardWorkflow(
      isOwner: Boolean,
      accessLevel: String,
      ownerName: String,
      workflow: Workflow,
      projectIDs: List[Integer],
      ownerId: Integer
  )

  case class WorkflowWithPrivilege(
      name: String,
      description: String,
      wid: Integer,
      content: String,
      creationTime: Timestamp,
      lastModifiedTime: Timestamp,
      isPublished: Boolean,
      readonly: Boolean
  )

  case class WorkflowIDs(wids: List[Integer], pid: Option[Integer])

  private def updateWorkflowField(
      workflow: Workflow,
      sessionUser: SessionUser,
      updateFunction: Workflow => Unit
  ): Unit = {
    val wid = workflow.getWid
    val user = sessionUser.getUser

    if (
      workflowOfUserExists(wid, user.getUid) || WorkflowAccessResource.hasWriteAccess(
        wid,
        user.getUid
      )
    ) {
      val userWorkflow = workflowDao.fetchOneByWid(wid)
      updateFunction(userWorkflow)
      workflowDao.update(userWorkflow)
    } else {
      throw new ForbiddenException("No sufficient access privilege.")
    }
  }

  /**
    * Updates operator IDs in the given workflow content by assigning new unique IDs.
    * Each operator ID in the "operators" section is replaced with a new ID of the form:
    * "<operatorType>-operator-<UUID>"
    *
    * @param workflowContent JSON string representing the workflow, containing operator details.
    * @return The updated workflow content with new operator IDs.
    */
  def assignNewOperatorIds(workflowContent: String): String = {
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val operatorIdMap = objectMapper
      .readValue(workflowContent, classOf[Map[String, List[Map[String, String]]]])("operators")
      .map(operator => {
        val oldOperatorId = operator("operatorID")
        val operatorType = operator("operatorType")
        // operator id in frontend: operatorSchema.operatorType + "-operator-" + uuid(); // v4 = UUID.randomUUID().toString
        val newOperatorId = s"$operatorType-operator-${UUID.randomUUID()}"
        oldOperatorId -> newOperatorId
      })
      .toMap

    // replace all old operator ids with new operator ids
    operatorIdMap.foldLeft(workflowContent) {
      case (updatedContent, (oldId, newId)) =>
        updatedContent.replace(oldId, newId)
    }
  }

  def baseWorkflowSelect(): SelectOnConditionStep[Record9[
    Integer,
    String,
    String,
    Timestamp,
    Timestamp,
    PrivilegeEnum,
    Integer,
    String,
    String
  ]] = {
    context
      .select(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.DESCRIPTION,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME,
        WORKFLOW_USER_ACCESS.PRIVILEGE,
        WORKFLOW_OF_USER.UID,
        USER.NAME,
        groupConcatDistinct(WORKFLOW_OF_PROJECT.PID).as("projects")
      )
      .from(WORKFLOW)
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .leftJoin(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .leftJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
  }

  def mapWorkflowEntries(
      workflowEntries: Result[Record9[
        Integer,
        String,
        String,
        Timestamp,
        Timestamp,
        PrivilegeEnum,
        Integer,
        String,
        String
      ]],
      uid: Integer
  ): List[DashboardWorkflow] = {
    workflowEntries
      .map(workflowRecord =>
        DashboardWorkflow(
          if (uid != null)
            workflowRecord.into(WORKFLOW_OF_USER).getUid.eq(uid)
          else false,
          workflowRecord
            .into(WORKFLOW_USER_ACCESS)
            .into(classOf[WorkflowUserAccess])
            .getPrivilege
            .toString,
          workflowRecord.into(USER).getName,
          workflowRecord.into(WORKFLOW).into(classOf[Workflow]),
          if (workflowRecord.component9() == null) List[Integer]()
          else
            workflowRecord.component9().split(',').map(str => Integer.valueOf(str)).toList,
          workflowRecord.into(WORKFLOW_OF_USER).getUid
        )
      )
      .asScala
      .toList
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/workflow")
class WorkflowResource extends LazyLogging {

  /**
    * This method returns all workflow IDs that the user has access to
    *
    * @return WorkflowID[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-workflow-ids")
  def retrieveIDs(@Auth user: SessionUser): util.List[String] = {
    context
      .select(WORKFLOW_USER_ACCESS.WID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  /**
    * This method returns all owner user names of the workflows that the user has access to
    *
    * @return OwnerName[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-workflow-owners")
  def retrieveOwners(@Auth user: SessionUser): util.List[String] = {
    context
      .selectDistinct(USER.EMAIL)
      .from(WORKFLOW_USER_ACCESS)
      .join(WORKFLOW_OF_USER)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW_OF_USER.WID))
      .join(USER)
      .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  /**
    * This method returns workflow IDs, that contain the selected operators, as strings
    *
    * @return WorkflowID[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/search-by-operators")
  def searchWorkflowByOperator(
      @QueryParam("operator") operator: String,
      @Auth sessionUser: SessionUser
  ): List[String] = {
    // Example GET url: localhost:8080/workflow/searchOperators?operator=Regex,CSVFileScan
    val user = sessionUser.getUser
    val quotes = "\""
    val operatorArray =
      operator.replace(" ", "").stripPrefix("[").stripSuffix("]").split(',')
    var orCondition: Condition = noCondition()
    for (i <- operatorArray.indices) {
      val operatorName = operatorArray(i)
      orCondition = orCondition.or(
        WORKFLOW.CONTENT
          .likeIgnoreCase(
            "%" + quotes + "operatorType" + quotes + ":" + quotes + s"$operatorName" + quotes + "%"
            //gives error when I try to combine escape character with formatted string
            //may be due to old scala version bug
          )
      )

    }

    val workflowEntries =
      context
        .select(
          WORKFLOW.WID
        )
        .from(WORKFLOW)
        .join(WORKFLOW_USER_ACCESS)
        .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
        .where(
          orCondition
            .and(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
        )
        .fetch()

    workflowEntries
      .map(workflowRecord => {
        workflowRecord.into(WORKFLOW).getWid.intValue().toString
      })
      .asScala
      .toList
  }

  /**
    * This method returns the current in-session user's workflow list based on all workflows he/she has access to
    *
    * @return Workflow[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def retrieveWorkflowsBySessionUser(
      @Auth sessionUser: SessionUser
  ): List[DashboardWorkflow] = {
    val user = sessionUser.getUser
    val workflowEntries = baseWorkflowSelect()
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .groupBy(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.DESCRIPTION,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME,
        WORKFLOW_USER_ACCESS.PRIVILEGE,
        WORKFLOW_OF_USER.UID,
        USER.NAME
      )
      .fetch()
    mapWorkflowEntries(workflowEntries, user.getUid)
  }

  /**
    * This method handles the client request to get a specific workflow to be displayed in canvas
    * at current design, it only takes the workflowID and searches within the database for the matching workflow
    * for future design, it should also take userID as an parameter.
    *
    * @param wid workflow id, which serves as the primary key in the UserWorkflow database
    * @return a json string representing an savedWorkflow
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{wid}")
  def retrieveWorkflow(
      @PathParam("wid") wid: Integer,
      @Auth user: SessionUser
  ): WorkflowWithPrivilege = {
    if (WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      val workflow = workflowDao.fetchOneByWid(wid)
      WorkflowWithPrivilege(
        workflow.getName,
        workflow.getDescription,
        workflow.getWid,
        workflow.getContent,
        workflow.getCreationTime,
        workflow.getLastModifiedTime,
        workflow.getIsPublic,
        !WorkflowAccessResource.hasWriteAccess(wid, user.getUid)
      )
    } else {
      throw new ForbiddenException("No sufficient access privilege.")
    }
  }

  /**
    * This method persists the workflow into database
    *
    * @param workflow , a workflow
    * @return Workflow, which contains the generated wid if not provided//
    *         TODO: divide into two endpoints -> one for new-workflow and one for updating existing workflow
    *         TODO: if the persist is triggered in parallel, the none atomic actions currently might cause an issue.
    *         Should consider making the operations atomic
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/persist")
  def persistWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): Workflow = {
    val user = sessionUser.getUser
    if (user == edu.uci.ics.texera.web.auth.GuestAuthFilter.GUEST) {
      throw new ForbiddenException("Guest user does not have access to db.")
    }

    if (workflowOfUserExists(workflow.getWid, user.getUid)) {
      WorkflowVersionResource.insertVersion(workflow, insertingNewWorkflow = false)
      workflowDao.update(workflow)
    } else {
      if (!WorkflowAccessResource.hasReadAccess(workflow.getWid, user.getUid)) {
        // not owner and no access record --> new record
        workflow.setWid(null)
        insertWorkflow(workflow, user)
        WorkflowVersionResource.insertVersion(workflow, insertingNewWorkflow = true)
      } else if (WorkflowAccessResource.hasWriteAccess(workflow.getWid, user.getUid)) {
        WorkflowVersionResource.insertVersion(workflow, insertingNewWorkflow = false)
        // not owner but has write access
        workflowDao.update(workflow)
      } else {
        // not owner and no write access -> rejected
        throw new ForbiddenException("No sufficient access privilege.")
      }
    }

    val wid = workflow.getWid
    workflowDao.fetchOneByWid(wid)
  }

  /**
    * This method duplicates the target workflow, the new workflow name is appended with `_copy`
    *
    * @param workflow , a workflow to be duplicated
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/duplicate")
  def duplicateWorkflow(
      workflowIDs: WorkflowIDs,
      @Auth sessionUser: SessionUser
  ): List[DashboardWorkflow] = {

    val user = sessionUser.getUser
    // do the permission check first
    for (wid <- workflowIDs.wids) {
      if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
        throw new ForbiddenException("No sufficient access privilege.")
      }
    }

    val resultWorkflows: ListBuffer[DashboardWorkflow] = ListBuffer()
    val addToProject = workflowIDs.pid.nonEmpty
    // then start a transaction and do the duplication
    try {
      context.transaction { txConfig =>
        for (wid <- workflowIDs.wids) {
          val oldWorkflow: Workflow = workflowDao.fetchOneByWid(wid)
          val newWorkflow = createWorkflow(
            new Workflow(
              null,
              oldWorkflow.getName + "_copy",
              oldWorkflow.getDescription,
              assignNewOperatorIds(oldWorkflow.getContent),
              null,
              null,
              false
            ),
            sessionUser
          )
          // if workflows also need to be added to the project
          if (addToProject) {
            val newWid = newWorkflow.workflow.getWid
            if (!hasReadAccess(newWid, user.getUid)) {
              throw new ForbiddenException("No sufficient access privilege to workflow.")
            }
            val pid = workflowIDs.pid.get
            if (!workflowOfProjectExists(newWid, pid)) {
              workflowOfProjectDao.insert(new WorkflowOfProject(newWid, pid))
            } else {
              throw new BadRequestException("Workflow already exists in the project")
            }
          }
          resultWorkflows += newWorkflow
        }
      }
    } catch {
      case _: BadRequestException | _: ForbiddenException =>
      case NonFatal(exception) =>
        throw new WebApplicationException(exception)
    }
    resultWorkflows.toList
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/clone/{wid}")
  def cloneWorkflow(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser,
      @Context request: HttpServletRequest
  ): Integer = {
    val oldWorkflow: Workflow = workflowDao.fetchOneByWid(wid)
    val newWorkflow: DashboardWorkflow = createWorkflow(
      new Workflow(
        null,
        oldWorkflow.getName + "_clone",
        oldWorkflow.getDescription,
        assignNewOperatorIds(oldWorkflow.getContent),
        null,
        null,
        false
      ),
      sessionUser
    )

    recordCloneAction(request, sessionUser.getUid, wid, EntityType.Workflow)

    newWorkflow.workflow.getWid
  }

  /**
    * This method creates and insert a new workflow to database
    *
    * @param workflow , a workflow to be created
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  def createWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): DashboardWorkflow = {
    val user = sessionUser.getUser
    if (workflow.getWid != null) {
      throw new BadRequestException("Cannot create a new workflow with a provided id.")
    } else {
      insertWorkflow(workflow, user)
      WorkflowVersionResource.insertVersion(workflow, insertingNewWorkflow = true)
      DashboardWorkflow(
        isOwner = true,
        PrivilegeEnum.WRITE.toString,
        user.getName,
        workflowDao.fetchOneByWid(workflow.getWid),
        List[Integer](),
        user.getUid
      )
    }

  }

  /**
    * Deletes workflows from the database and cleans up associated resources.
    *
    * @param workflowIDs The IDs of workflows to delete
    * @param sessionUser Current authenticated user
    * @return Unit, with appropriate HTTP status: 200 if deleted, 400 if not exists
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/delete")
  def deleteWorkflow(workflowIDs: WorkflowIDs, @Auth sessionUser: SessionUser): Unit = {
    val user = sessionUser.getUser

    try {
      // Find all execution IDs related to these workflows
      val eids = context
        .select(WORKFLOW_EXECUTIONS.EID)
        .from(WORKFLOW_EXECUTIONS)
        .join(WORKFLOW_VERSION)
        .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
        .join(WORKFLOW)
        .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW.WID.in(workflowIDs.wids.asJava))
        .fetchInto(classOf[Integer])
        .asScala
        .toList

      // Collect all URIs related to executions for cleanup
      val uris = eids.flatMap { eid =>
        val executionId = ExecutionIdentity(eid.longValue())

        // Gather URIs from all execution resources
        val resultUris = WorkflowExecutionsResource.getResultUrisByExecutionId(executionId)
        val consoleMessagesUris =
          WorkflowExecutionsResource.getConsoleMessagesUriByExecutionId(executionId)
        val runtimeStatsUris =
          WorkflowExecutionsResource.getRuntimeStatsUriByExecutionId(executionId).toList

        resultUris ++ consoleMessagesUris ++ runtimeStatsUris
      }

      // Delete workflows in a transaction
      context.transaction { _ =>
        for (wid <- workflowIDs.wids) {
          if (workflowOfUserExists(wid, user.getUid)) {
            workflowDao.deleteById(wid)
          } else {
            throw new BadRequestException("The workflow does not exist.")
          }
        }
      }

      // Clean up document storage
      try {
        uris.foreach { uri =>
          try {
            val (document, _) = DocumentFactory.openDocument(uri)
            document.clear()
          } catch {
            case e: IllegalArgumentException if e.getMessage.contains("No storage is found") =>
              logger.warn(s"Storage for URI $uri not found, ignoring: ${e.getMessage}")
            case NonFatal(e) =>
              logger.error(s"Failed to clear document for URI $uri", e)
          }
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Failed to clean up execution results", e)
      }
    } catch {
      case _: BadRequestException =>
      case NonFatal(exception)    => throw new WebApplicationException(exception)
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/name")
  def updateWorkflowName(
      workflow: Workflow,
      @Auth sessionUser: SessionUser
  ): Unit = {
    updateWorkflowField(workflow, sessionUser, _.setName(workflow.getName))
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateWorkflowDescription(
      workflow: Workflow,
      @Auth sessionUser: SessionUser
  ): Unit = {
    updateWorkflowField(workflow, sessionUser, _.setDescription(workflow.getDescription))
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/public/{wid}")
  def makePublic(@PathParam("wid") wid: Integer, @Auth user: SessionUser): Unit = {
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException(s"You do not have permission to modify workflow $wid")
    }
    val workflow: Workflow = workflowDao.fetchOneByWid(wid)
    workflow.setIsPublic(true)
    workflowDao.update(workflow)
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/private/{wid}")
  def makePrivate(@PathParam("wid") wid: Integer, @Auth user: SessionUser): Unit = {
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException(s"You do not have permission to modify workflow $wid")
    }
    val workflow: Workflow = workflowDao.fetchOneByWid(wid)
    workflow.setIsPublic(false)
    workflowDao.update(workflow)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/type/{wid}")
  def getWorkflowType(@PathParam("wid") wid: Integer): String = {
    val workflow: Workflow = workflowDao.fetchOneByWid(wid)
    if (workflow.getIsPublic) {
      "Public"
    } else {
      "Private"
    }
  }

  @GET
  @Path("/owner_user")
  def getOwnerUser(@QueryParam("wid") wid: Integer): User = {
    context
      .select(
        USER.UID,
        USER.NAME,
        USER.EMAIL,
        USER.PASSWORD,
        USER.GOOGLE_ID,
        USER.ROLE,
        USER.GOOGLE_AVATAR
      )
      .from(WORKFLOW_OF_USER)
      .join(USER)
      .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
      .where(WORKFLOW_OF_USER.WID.eq(wid))
      .fetchOneInto(classOf[User])
  }

  @GET
  @Path("/workflow_name")
  def getWorkflowName(@QueryParam("wid") wid: Integer): String = {
    context
      .select(
        WORKFLOW.NAME
      )
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[String])
  }

  @GET
  @Path("/publicised/{wid}")
  def retrievePublicWorkflow(
      @PathParam("wid") wid: Integer
  ): WorkflowWithPrivilege = {
    val workflow = workflowDao.ctx
      .selectFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .and(WORKFLOW.IS_PUBLIC.isTrue)
      .fetchOne()
    WorkflowWithPrivilege(
      workflow.getName,
      workflow.getDescription,
      workflow.getWid,
      workflow.getContent,
      workflow.getCreationTime,
      workflow.getLastModifiedTime,
      workflow.getIsPublic,
      readonly = true
    )
  }

  @GET
  @Path("/workflow_description")
  def getWorkflowDescription(@QueryParam("wid") wid: Integer): String = {
    context
      .select(
        WORKFLOW.DESCRIPTION
      )
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[String])
  }

  //TODO Get size from database
  @GET
  @Path("/size")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getSize(@QueryParam("wid") wids: java.util.List[Integer]): java.util.Map[Integer, Int] = {
    val result = new java.util.HashMap[Integer, Int]()
    if (wids != null && !wids.isEmpty) {
      workflowDao.ctx
        .selectFrom(WORKFLOW)
        .where(WORKFLOW.WID.in(wids))
        .fetch()
        .asScala
        .foreach { wf =>
          result.put(wf.getWid, wf.getContent.length)
        }
    }
    result
  }
}
