/**
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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, switchMap, map, finalize, throwError } from "rxjs";
import { AgentService } from "../agent/agent.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";

export interface DocEntry {
  markdown: string;
  generatedAt: Date;
}

@Injectable({
  providedIn: "root",
})
export class WorkflowDocService {
  private readonly AGENT_API_BASE = "/api";
  private cache = new Map<number | undefined, DocEntry>();

  constructor(
    private http: HttpClient,
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService
  ) {}

  getCached(wid: number | undefined): DocEntry | null {
    return this.cache.get(wid) ?? null;
  }

  generateDocumentation(): Observable<string> {
    const wid = this.workflowActionService.getWorkflow()?.wid;
    const workflowContent = this.workflowActionService.getWorkflowContent();
    let tempAgentId: string | null = null;

    return this.agentService.fetchModelTypes().pipe(
      switchMap(modelTypes => {
        if (modelTypes.length === 0) {
          return throwError(() => new Error("No AI models are available. Please contact your administrator."));
        }
        return this.agentService.createAgent(modelTypes[0].id, "doc-agent");
      }),
      switchMap(agent => {
        tempAgentId = agent.id;
        return this.http
          .post<{ markdown: string }>(`${this.AGENT_API_BASE}/agents/${agent.id}/document-workflow`, {
            workflowContent,
          })
          .pipe(
            map(response => {
              this.cache.set(wid, { markdown: response.markdown, generatedAt: new Date() });
              return response.markdown;
            }),
            finalize(() => {
              if (tempAgentId) {
                this.agentService.deleteAgent(tempAgentId).subscribe();
                tempAgentId = null;
              }
            })
          );
      })
    );
  }
}
