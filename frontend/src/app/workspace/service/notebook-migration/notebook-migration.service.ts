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

import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Injectable } from "@angular/core";
import { AppSettings } from "../../../common/app-setting";
import { Notebook, NotebookMigrationLLM } from "./migration-llm";
import { firstValueFrom } from "rxjs";
import { environment } from "../../../../environments/environment";
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

@UntilDestroy()
@Injectable({
  providedIn: "root",
})
export class NotebookMigrationService {
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
    const jupyterAPIUrl = `${environment.notebookMigrationFastAPIUrl}/jupyter/set_notebook`;

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
      this.notificationService.success("Notebook opened successfully in Jupyter");
      return 1;
    } catch (error) {
      console.error("Error sending notebook to pod: ", error);
      // @ts-ignore
      this.notificationService.error("Error sending notebook to Jupyter: " + error.message);
      return 0;
    }
  }

  public storeNotebookAndMapping(wid: number | undefined, vid: number = 1, mappingContent: any, notebookContent: any) {
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/store-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });
    const payload = {
      wid: wid,
      vid: vid,
      mapping: mappingContent,
      notebook: notebookContent,
    };

    this.http
      .post(dbAPIUrl, payload, { headers })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (response: any) => {
          console.log("wid, mapping, and notebook stored in migration database:", response?.message);
        },
        error: (error: unknown) => {
          console.error("Network response was not ok", error);
        },
      });
  }
}
