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

import { UntilDestroy } from "@ngneat/until-destroy";
import { Injectable } from "@angular/core";
import { AppSettings } from "../../../common/app-setting";
import { Notebook, NotebookMigrationLLM } from "./migration-llm";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { catchError, firstValueFrom, map, Observable, of } from "rxjs";

interface LiteLLMModel {
  id: string;
  object: string;
  created: number;
  owned_by: string;
}

interface LiteLLMModelsResponse {
  data: LiteLLMModel[];
  object: string;
}

interface MappingContent {
  cell_to_operator: { [key: string]: any };
  operator_to_cell: { [key: string]: any };
}

@UntilDestroy()
@Injectable({
  providedIn: "root",
})
export class NotebookMigrationService {
  private mapping: { [key: string]: MappingContent } = {
    default: {
      cell_to_operator: {},
      operator_to_cell: {},
    },
  };

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

  public getAvailableModels(): Observable<{ name: string }[]> {
    return this.http
      .get<LiteLLMModelsResponse>(`${AppSettings.getApiEndpoint()}/models`)
      .pipe(
        map(response =>
          response.data.map(model => ({
            name: model.id
          }))
        ),
        catchError(err => {
          console.error('Failed to fetch models', err);
          return of([]);
        })
      );
  }

  public async sendToAIGenerateWorkflow(notebookContent: Notebook, modelType: string, apiKey: string) {
    const migrationLLM = new NotebookMigrationLLM();
    migrationLLM.initialize(modelType, apiKey);

    const isValid = await migrationLLM.verifyConnection();
    if (!isValid) {
      throw new Error("Invalid API key or backend connection");
    }

    try {
      const result = await firstValueFrom(await migrationLLM.convertNotebookToWorkflow(notebookContent));
      const parsedResult = JSON.parse(result);
      const workflowContent = parsedResult.workflowJSON;
      const mappingContent = parsedResult.workflowNotebookMapping;
      return { workflowContent, mappingContent };
    } catch (error) {
      console.error("Error converting notebook:", error);
    } finally {
      migrationLLM.close();
    }
  }

  public async sendNotebookToJupyter(notebookData: Notebook) {
    const jupyterAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/set-notebook`;

    const requestBody = {
      notebookName: "notebook.ipynb",
      notebookData: notebookData,
    };

    const headers = new HttpHeaders({
      "Content-Type": "application/json",
    });

    try {
      const response: any = await firstValueFrom(this.http.post(jupyterAPIUrl, requestBody, { headers }));
      console.log("Notebook successfully sent to Jupyter:", response);
      this.notificationService.success("Notebook successfully sent to Jupyter");
      return 1;
    } catch (error) {
      console.error("Error sending notebook to pod: ", error);
      // @ts-ignore
      this.notificationService.error("Error sending notebook to Jupyter: " + error.message);
      return 0;
    }
  }

  public async getJupyterURL(): Promise<string | null> {
    try {
      const response = await fetch("/api/notebook-migration/get-jupyter-url");
      if (!response.ok) {
        console.error("Failed to get Jupyter URL:", response.status);
        return null;
      }

      const data = await response.json() as { success: boolean; url?: string };

      if (!data.success || !data.url) {
        console.error("Jupyter server unavailable");
        return null;
      }

      return data.url;

    } catch (err) {
      console.error("Error fetching Jupyter URL:", err);
      return null;
    }
  }

  public async getJupyterIframeURL(): Promise<string | null> {
    try {
      const response = await fetch("/api/notebook-migration/get-jupyter-iframe-url");
      if (!response.ok) {
        console.error("Failed to get Jupyter iframe URL:", response.status);
        return null;
      }

      const data = await response.json() as { success: boolean; url?: string };

      if (!data.success || !data.url) {
        console.error("Jupyter server unavailable");
        return null;
      }

      return data.url;

    } catch (err) {
      console.error("Error fetching Jupyter iframe URL:", err);
      return null;
    }
  }

  public storeNotebookAndMapping(
    wid: number | undefined,
    vid: number = 1,
    mappingContent: any,
    notebookContent: any
  ) {
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/store-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });

    const payload = {
      wid,
      vid,
      mapping: mappingContent,
      notebook: notebookContent,
    };

    return this.http.post(dbAPIUrl, payload, { headers });
  }

  public hasMapping(id:string): boolean {
    return id in this.mapping;
  }

  public getMapping(id: string): MappingContent | undefined {
    return this.mapping[id];
  }

  public setMapping(id: string, value: MappingContent): void {
    this.mapping[id] = value;
  }

  public deleteMapping(id: string): void {
    delete this.mapping[id];
  }
}
