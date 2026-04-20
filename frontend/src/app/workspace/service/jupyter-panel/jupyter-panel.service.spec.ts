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

import { TestBed } from "@angular/core/testing";
import { JupyterPanelService } from "./jupyter-panel.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { NotebookMigrationService } from "../notebook-migration/notebook-migration.service";
import { of } from "rxjs";

describe("JupyterPanelService", () => {
  let service: JupyterPanelService;
  let httpMock: HttpTestingController;

  let mockWorkflow: any;
  let mockNotification: any;
  let mockNotebook: any;

  beforeEach(() => {
    mockWorkflow = {
      workflowMetaDataChanged: jasmine.createSpy().and.returnValue(of({ wid: 1 })),
      getWorkflow: jasmine.createSpy().and.returnValue({ wid: 1 }),
      getTexeraGraph: jasmine.createSpy().and.returnValue({
        getAllLinks: () => [
          {
            linkID: "L1",
            source: { operatorID: "A" },
            target: { operatorID: "B" },
          },
        ],
        getAllOperators: () => [{ operatorID: "A" }, { operatorID: "B" }],
      }),
      highlightOperators: jasmine.createSpy(),
      highlightLinks: jasmine.createSpy(),
      unhighlightOperators: jasmine.createSpy(),
      unhighlightLinks: jasmine.createSpy(),
    };

    mockNotification = {
      warning: jasmine.createSpy(),
    };

    mockNotebook = {
      hasMapping: jasmine.createSpy().and.returnValue(true),
      getMapping: jasmine.createSpy().and.returnValue({
        cell_to_operator: {
          cell1: ["A", "B"],
        },
        operator_to_cell: {},
      }),
      deleteMapping: jasmine.createSpy(),
      setMapping: jasmine.createSpy(),
      getJupyterURL: jasmine.createSpy().and.resolveTo("http://jupyter"),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        JupyterPanelService,
        { provide: WorkflowActionService, useValue: mockWorkflow },
        { provide: NotificationService, useValue: mockNotification },
        { provide: NotebookMigrationService, useValue: mockNotebook },
      ],
    });

    service = TestBed.inject(JupyterPanelService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  // Panel visibility
  it("should open and close panel", () => {
    let state: boolean | null = null;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openPanel("JupyterNotebookPanel");
    expect(state).toBeTrue();

    service.closeJupyterNotebookPanel();
    expect(state).toBeFalse();
  });

  it("should minimize panel", () => {
    let state: boolean | null = true;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.minimizeJupyterNotebookPanel();

    expect(state).toBeFalse();
  });

  // openJupyterNotebookPanel
  it("should warn if no mapping exists", () => {
    mockNotebook.hasMapping.and.returnValue(false);

    service.openJupyterNotebookPanel();

    expect(mockNotification.warning).toHaveBeenCalled();
  });

  it("should open panel if mapping exists", () => {
    mockNotebook.hasMapping.and.returnValue(true);

    let state: boolean | null = false;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openJupyterNotebookPanel();

    expect(state).toBeTrue();
  });

  // HTTP fetchNotebookAndMapping
  it("should return 0 when exists=false", (done) => {
    (service as any)
      .fetchNotebookAndMapping(1, 1)
      .subscribe((result: any) => {
        expect(result).toBe(0);
        done();
      });

    const req = httpMock.expectOne(r =>
      r.url.includes("/notebook-migration/fetch-notebook-and-mapping")
    );

    req.flush({ exists: false });
  });

  // iframe ref
  it("should store iframe reference", () => {
    const iframe = document.createElement("iframe");

    service.setIframeRef(iframe);

    expect((service as any).iframeRef).toBe(iframe);
  });

  // highlightFromCell
  it("should highlight operators and links", () => {
    (service as any).cellToHighlightMapping = {
      cell1: {
        components: ["op1", "op2"],
        edges: ["link1"]
      }
    };

    const method = (service as any).highlightFromCell.bind(service);

    method("cell1");

    expect(mockWorkflow.unhighlightOperators).toHaveBeenCalled();
    expect(mockWorkflow.unhighlightLinks).toHaveBeenCalled();
    expect(mockWorkflow.highlightOperators).toHaveBeenCalledWith(true, "op1", "op2");
    expect(mockWorkflow.highlightLinks).toHaveBeenCalledWith(true, "link1");
  });

  // openPanel
  it("should open panel only for correct name", () => {
    let state: boolean | null = false;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openPanel("WrongPanel");
    expect(state).toBeFalse();

    service.openPanel("JupyterNotebookPanel");
    expect(state).toBeTrue();
  });

  // onWorkflowComponentClick
  it("should postMessage when mapping exists", async () => {
    const mockIframe = {
      contentWindow: {
        postMessage: jasmine.createSpy(),
      },
    } as any;

    service.setIframeRef(mockIframe);
    (mockNotebook as any).getMapping.and.returnValue({
      cell_to_operator: {},
      operator_to_cell: {
        cell1: ["op1", "op2"]
      }
    });

    await service.onWorkflowComponentClick("cell1");

    expect(mockIframe.contentWindow.postMessage).toHaveBeenCalledWith(
      {
        action: "triggerCellClick",
        operators: ["op1", "op2"]
      },
      "http://jupyter"
    );
  });
});
