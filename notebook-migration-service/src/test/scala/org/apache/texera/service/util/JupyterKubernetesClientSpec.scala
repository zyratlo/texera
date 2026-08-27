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

import org.apache.texera.common.config.KubernetesConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Covers the pure naming and addressing, which is what the registry stores and the service
  * dials. The fabric8-backed calls need a cluster and are left to deployment.
  */
class JupyterKubernetesClientSpec extends AnyFlatSpec with Matchers {

  private val client = new JupyterKubernetesClient(null)

  "generatePodName" should "namespace the pod by uid" in {
    client.generatePodName(7) shouldBe "jupyter-7"
  }

  it should "give every user a distinct pod name" in {
    (1 to 50).map(client.generatePodName).distinct.size shouldBe 50
  }

  "generatePodURI" should "address the pod through the headless service" in {
    // Must match the pod's hostname.subdomain, or the name does not resolve in-cluster.
    client.generatePodURI(7) shouldBe
      s"jupyter-7.${KubernetesConfig.jupyterServiceName}.${KubernetesConfig.jupyterNamespace}" +
        s".svc.cluster.local:${KubernetesConfig.jupyterPortNumber}"
  }

  it should "carry the configured port" in {
    client.generatePodURI(7) should endWith(s":${KubernetesConfig.jupyterPortNumber}")
  }
}
