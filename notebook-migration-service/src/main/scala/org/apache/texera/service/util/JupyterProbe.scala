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

package org.apache.texera.service.util

import java.net.{HttpURLConnection, URL}

/** Liveness check for a Jupyter server. */
object JupyterProbe {

  private val timeoutMillis = 2000

  /**
    * Whether Jupyter answers on `internalUrl`. /api returns the server version without a
    * token, so 403 counts as reachable: the server is up and merely refusing the request.
    */
  def isAvailable(internalUrl: String): Boolean = {
    var conn: HttpURLConnection = null
    try {
      conn = new URL(s"$internalUrl/api").openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setConnectTimeout(timeoutMillis)
      conn.setReadTimeout(timeoutMillis)
      val status = conn.getResponseCode
      status == 200 || status == 403
    } catch {
      case _: Exception => false
    } finally {
      if (conn != null) conn.disconnect()
    }
  }
}
