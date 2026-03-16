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
import { BehaviorSubject, catchError, map, Observable, of } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import mapping from "../../../../assets/migration_tool/mapping";
import { OperatorLink } from "../../types/workflow-common.interface";
import { environment } from "../../../../environments/environment";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { UntilDestroy } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { distinctUntilChanged, switchMap } from "rxjs/operators";
import { AppSettings } from "../../../common/app-setting";

@UntilDestroy()
@Injectable({
  providedIn: "root",
})
export class JupyterPanelService {
  private jupyterNotebookPanelVisible = new BehaviorSubject<boolean>(false);
  public jupyterNotebookPanelVisible$ = this.jupyterNotebookPanelVisible.asObservable();

  private iframeRef: HTMLIFrameElement | null = null; // Store reference to iframe element
  private cellContent: string[] = []; // Store the content of the cells
  private highlightedCell: number | null = null; // Track the highlighted cell

  // Precomputed dictionary for cell to highlight mapping
  private cellToHighlightMapping: Record<string, { components: string[]; edges: string[] }> = {};

  constructor(
    private workflowActionService: WorkflowActionService,
    private http: HttpClient,
    private notificationService: NotificationService
  ) {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(
        map(meta => meta.wid),
        distinctUntilChanged()
      )
      .subscribe(wid => {
        console.log("Checking for existing notebook and mapping...");
        this.fetchNotebookAndMapping(wid).subscribe(result => {
          if (result) {
            console.log("Workflow graph updated, recomputing highlight mapping...");
            this.precomputeHighlightMapping();
            this.openJupyterNotebookPanel();
          } else {
            this.closeJupyterNotebookPanel();
            console.log("No existing notebook and mapping found.")
          }
        });
      });
    window.addEventListener("message", this.handleNotebookMessage);
  }

  private fetchNotebookAndMapping(workflowID: number | undefined = this.workflowActionService.getWorkflow().wid, vId: number = 1): Observable<number> {
    // Fetch mapping and notebook from migration database if exists for wid
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/fetch-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });
    const payload = {
      wid: workflowID,
      vid: vId, // Future work: add dynamic fetching of current workflow vId
    };

    return this.http
      .post(dbAPIUrl, payload, { headers })
      .pipe(
        switchMap((response: any) => {
          // Only load mapping and workflow if they exist
          if(response.exists) {
            const mappingID = "mapping_wid_" + workflowID;
            mapping[mappingID] = response.mapping;
            const jupyterAPIUrl = `${environment.notebookMigrationFastAPIUrl}/jupyter/set_notebook`;

            const requestBody = {
              notebookName: "notebook.ipynb",
              notebookData: response.notebook,
            };

            const headers = new HttpHeaders({
              "Content-Type": "application/json",
            });

            return this.http.post(jupyterAPIUrl, requestBody, { headers }).pipe(
              map(() => {
                this.notificationService.success("Found existing Jupyter notebook and mapping.");
                return 1;
              }),
              catchError((error) => {
                // Handle the error
                console.error("Error sending notebook to pod: ", error);
                this.notificationService.error("Error sending notebook to JupyterLab: " + error.message);
                return of(0);
              })
            );
          } else {
            return of(0);
          }
        }),
        catchError((error: unknown) => {
          console.error("Network response was not ok", error);
          return of(0);
        })
      );
  }

  // Precompute the dictionary for O(1) highlighting
  private precomputeHighlightMapping(): void {
    const wid = this.workflowActionService.getWorkflow().wid;

    if (wid === undefined) {
      console.warn("Workflow ID is undefined. Cannot compute highlight mapping.");
      return;
    }
    const mappingKey = "mapping_wid_" + wid;

    if (!(mappingKey in mapping)) {
      console.warn(`Mapping key '${mappingKey}' not found. Cannot compute highlight mapping.`);
      return;
    }
    const cellToOperator = mapping[mappingKey].cell_to_operator;

    const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
    if (allLinks.length === 0) {
      console.warn("No links found in the graph during precompute.");
      return;
    }

    for (const cellUUID in cellToOperator) {
      const components = cellToOperator[cellUUID] || [];
      const componentSet = new Set(components);
      const edges: string[] = [];

      allLinks.forEach(link => {
        const sourceOperatorID = link.source.operatorID;
        const targetOperatorID = link.target.operatorID;

        if (
          componentSet.has(sourceOperatorID) &&
          componentSet.has(targetOperatorID) &&
          sourceOperatorID !== targetOperatorID
        ) {
          edges.push(link.linkID);
        }
      });

      this.cellToHighlightMapping[cellUUID] = { components, edges };
    }
  }

  // Set the iframe reference (from the component's ViewChild)
  setIframeRef(iframe: HTMLIFrameElement) {
    this.iframeRef = iframe;
    this.iframeRef.onload = () => console.log("Iframe loaded successfully.");
  }

  // Open the Jupyter Notebook panel
  openPanel(panelName: string): void {
    if (panelName === "JupyterNotebookPanel") {
      this.jupyterNotebookPanelVisible.next(true);
    }
  }

  // Close the Jupyter Notebook panel
  closeJupyterNotebookPanel(): void {
    this.jupyterNotebookPanelVisible.next(false);
    const wid = this.workflowActionService.getWorkflow().wid;
    if (wid != undefined && "mapping_wid_" + wid in mapping) {
      delete mapping["mapping_wid_" + wid];
    }
  }

  // Minimize the Jupyter Notebook panel
  public minimizeJupyterNotebookPanel(): void {
    this.jupyterNotebookPanelVisible.next(false);
  }

  // Expand the Jupyter Notebook panel
  public openJupyterNotebookPanel(): void {
    const wid = this.workflowActionService.getWorkflow().wid;
    const mappingKey = "mapping_wid_" + wid;

    // Check if there is corresponding mapping data
    if (wid === undefined || !(mappingKey in mapping)) {
      console.warn("No Jupyter notebook found for this workflow. Cannot open panel.");
      this.notificationService.warning("No Jupyter notebook associated with this workflow.");
      return;
    }

    // Expand only if the mapping exists
    this.jupyterNotebookPanelVisible.next(true);
  }

  // Handle messages from the Jupyter notebook iframe
  private handleNotebookMessage = (event: MessageEvent) => {
    const allowedOrigins = [window.location.origin, environment.jupyterAPIUrl];
    if (!allowedOrigins.includes(event.origin)) {
      console.log("Invalid origin:", event.origin);
      return;
    }

    const { action, cellIndex, cellContent, cellUUID } = event.data;

    if (action === "cellClicked") {
      this.highlightedCell = cellIndex;
      this.cellContent[cellIndex] = cellContent || `Cell ${cellIndex + 1}`;
      this.highlightFromCell(cellUUID);
    }
  };

  // Highlight operators and edges based on the clicked cell
  private highlightFromCell(cellUUID: string): void {
    const highlightData = this.cellToHighlightMapping[cellUUID] || { components: [], edges: [] };

    // Unhighlight all operators and links
    this.workflowActionService.unhighlightOperators(
      ...this.workflowActionService
        .getTexeraGraph()
        .getAllOperators()
        .map(op => op.operatorID)
    );
    this.workflowActionService.unhighlightLinks(
      ...this.workflowActionService
        .getTexeraGraph()
        .getAllLinks()
        .map(link => link.linkID)
    );

    // Highlight components and edges
    if (highlightData.components.length > 0) {
      this.workflowActionService.highlightOperators(true, ...highlightData.components);
    }
    if (highlightData.edges.length > 0) {
      this.workflowActionService.highlightLinks(true, ...highlightData.edges);
    }

    console.log(`Highlighted components: ${highlightData.components}, edges: ${highlightData.edges}`);
  }

  // Handle when a Texera component is clicked to trigger the corresponding notebook cell
  onWorkflowComponentClick(cellUUID: string): void {
    if (this.iframeRef && this.iframeRef.contentWindow) {
      const wid = this.workflowActionService.getWorkflow().wid;
      const operatorArray = mapping[wid != undefined ? "mapping_wid_" + wid : "default"]["operator_to_cell"][cellUUID];
      if (operatorArray) {
        this.iframeRef.contentWindow.postMessage(
          {action: "triggerCellClick", operators: operatorArray},
          environment.jupyterAPIUrl
        );
      } else {
        console.error(`No operators found for cellUUID: ${cellUUID}`);
      }
    }
  }
}
