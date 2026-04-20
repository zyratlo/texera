// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.apache.texera.dao.SqlServer
import org.jooq.JSONB
import org.apache.texera.dao.jooq.generated.tables.Notebook
import org.apache.texera.dao.jooq.generated.tables.WorkflowNotebookMapping
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

object NotebookMigrationResource extends LazyLogging {

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val jupyterUrl = sys.env.getOrElse("JUPYTER_API_URL", "http://localhost:9100")
  private var jupyterIframeURL = s"$jupyterUrl/notebooks/work/notebook.ipynb"

  private def isJupyterAvailable(jupyterUrl: String): Boolean = {
    try {
      val conn = new java.net.URL(s"$jupyterUrl/api")
        .openConnection()
        .asInstanceOf[java.net.HttpURLConnection]

      conn.setRequestMethod("GET")
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)

      val status = conn.getResponseCode

      status == 200 || status == 403
    } catch {
      case _: Exception => false
    }
  }

  // Returns the Jupyter iframe reference URL
  def getJupyterIframeURL(): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return Response.status(500).entity(
        """
      {
        "success": false,
        "message": "Cannot connect to Jupyter server"
      }
      """
      ).build()
    }

    Response.ok(
      s"""
    {
      "success": true,
      "url": "$jupyterIframeURL"
    }
    """
    ).build()
  }

  // Returns the URL of Jupyter
  def getJupyterURL(): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return Response.status(500).entity(
        """
      {
        "success": false,
        "message": "Cannot connect to Jupyter server"
      }
      """
      ).build()
    }

    Response.ok(
      s"""
    {
      "success": true,
      "url": "$jupyterUrl"
    }
    """
    ).build()
  }

  // Set the notebook in Jupyter
  def setNotebook(body: String): Response = {
    if (!isJupyterAvailable(jupyterUrl)) {
      return Response.status(500).entity(
        """
      {
        "success": false,
        "message": "Cannot connect to Jupyter server"
      }
      """
      ).build()
    }

    try {
      val json = mapper.readTree(body)

      val notebookName = json.get("notebookName").asText()
      val notebookData = json.get("notebookData")

      // Construct Jupyter API URL
      val apiUrl = s"$jupyterUrl/api/contents/work/$notebookName"

      val url = new URL(apiUrl)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]

      conn.setRequestMethod("PUT")
      conn.setDoOutput(true)
      conn.setRequestProperty("Content-Type", "application/json")

      val requestBody =
        s"""
      {
        "type": "notebook",
        "content": $notebookData
      }
      """

      val os = conn.getOutputStream
      os.write(requestBody.getBytes(StandardCharsets.UTF_8))
      os.flush()
      os.close()

      val status = conn.getResponseCode

      if (status != 200 && status != 201) {
        return Response.status(500).entity(
          s"""
        {
          "success": false,
          "message": "Failed to upload notebook to Jupyter (status $status)"
        }
        """
        ).build()
      }

      jupyterIframeURL = s"$jupyterUrl/notebooks/work/notebook.ipynb"

      Response.ok(
        s"""
      {
        "success": true,
        "message": "Notebook successfully sent to Jupyter."
      }
      """
      ).build()

    } catch {
      case NonFatal(e) =>
        logger.error("Error sending notebook to Jupyter", e)
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"${e.getMessage}"}""")
          .build()
    }
  }

  // Store notebook + mapping in database
  def storeNotebookAndMapping(body: String): Response = {
    try {
      val json = mapper.readTree(body)

      val wid: java.lang.Integer = json.get("wid").asInt()
      val vid: java.lang.Integer = json.get("vid").asInt()
      val mappingNode = json.get("mapping")
      val notebookNode = json.get("notebook")

      val dsl = SqlServer.getInstance().createDSLContext()

      val nid: java.lang.Integer = SqlServer.withTransaction(dsl) { ctx =>
        // Insert notebook
        val notebookRecord = ctx.insertInto(Notebook.NOTEBOOK)
          .set(Notebook.NOTEBOOK.WID, wid)
          .set(Notebook.NOTEBOOK.NOTEBOOK_, JSONB.valueOf(notebookNode.toString))
          .returning(Notebook.NOTEBOOK.NID)
          .fetchOne()

        val nidInside: java.lang.Integer = notebookRecord.getValue(Notebook.NOTEBOOK.NID)

        // Insert workflow-notebook mapping
        ctx.insertInto(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.WID, wid)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.VID, vid)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.NID, nidInside)
          .set(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING, JSONB.valueOf(mappingNode.toString))
          .execute()

        nidInside
      }

      Response.ok(
        s"""
      {
        "success": true,
        "message": "Notebook and mapping successfully stored. wid: $wid, vid: $vid, nid: $nid"
      }
      """
      ).build()

    } catch {
      case NonFatal(e) =>
        logger.error("Error storing mapping and workflow", e)
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"${e.getMessage}"}""")
          .build()
    }
  }


  // Fetch notebook + mapping
  def fetchNotebookAndMapping(body: String): Response = {
    try {
      val json = mapper.readTree(body)

      val wid: java.lang.Integer = json.get("wid").asInt()
      val vid: java.lang.Integer = json.get("vid").asInt()

      val dsl = SqlServer.getInstance().createDSLContext()

      // Fetch the most recent notebook (highest nid) for this workflow version
      val result = dsl.select(
          Notebook.NOTEBOOK.NID,
          Notebook.NOTEBOOK.NOTEBOOK_,
          WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING
        )
        .from(Notebook.NOTEBOOK)
        .join(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING)
        .on(Notebook.NOTEBOOK.WID.eq(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.WID))
        .and(Notebook.NOTEBOOK.NID.eq(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.NID))
        .where(Notebook.NOTEBOOK.WID.eq(wid))
        .and(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.VID.eq(vid))
        .orderBy(Notebook.NOTEBOOK.NID.desc()) // most recent nid first
        .limit(1)                             // only take the latest
        .fetchOne()

      if (result == null) {
        Response.ok("""{"exists": false}""").build()
      } else {
        val nid: Int = result.getValue(Notebook.NOTEBOOK.NID)
        val notebookJson: String = result.get(Notebook.NOTEBOOK.NOTEBOOK_).asInstanceOf[JSONB].data()
        val mappingJson: String = result.get(WorkflowNotebookMapping.WORKFLOW_NOTEBOOK_MAPPING.MAPPING).asInstanceOf[JSONB].data()

        Response.ok(
          s"""
        {
          "exists": true,
          "notebook": $notebookJson,
          "mapping": $mappingJson
        }
        """
        ).build()
      }

    } catch {
      case NonFatal(e) =>
        logger.error("Database error retrieving mapping", e)
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(s"""{"error":"${e.getMessage}"}""")
          .build()
    }
  }
}

@Path("/notebook-migration")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class NotebookMigrationResource extends LazyLogging {

  @GET
  @Path("/get-jupyter-iframe-url")
  def getJupyterIframeURL: Response = {
    logger.info("Getting Jupyter iframe URL")
    NotebookMigrationResource.getJupyterIframeURL()
  }

  @GET
  @Path("/get-jupyter-url")
  def getJupyterURL: Response = {
    logger.info("Getting Jupyter API URL")
    NotebookMigrationResource.getJupyterURL()
  }

  @POST
  @Path("/set-notebook")
  def setNotebook(body: String): Response = {
    logger.info("Setting notebook, request body: " + body)
    NotebookMigrationResource.setNotebook(body)
  }

  @POST
  @Path("/store-notebook-and-mapping")
  def storeNotebookAndMapping(body: String): Response = {
    logger.info("Storing notebook and mapping, request body: " + body)
    NotebookMigrationResource.storeNotebookAndMapping(body)
  }

  @POST
  @Path("/fetch-notebook-and-mapping")
  def fetchNotebookAndMapping(body: String): Response = {
    logger.info("Fetching notebook and mapping, request body: " + body)
    NotebookMigrationResource.fetchNotebookAndMapping(body)
  }
}