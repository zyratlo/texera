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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { JupyterNotebookPanelComponent } from "./jupyter-notebook-panel.component";
import { JupyterPanelService } from "../../service/jupyter-panel/jupyter-panel.service";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import { BrowserModule } from "@angular/platform-browser";
import { Subject } from "rxjs";
import { ElementRef } from "@angular/core";

describe("JupyterNotebookPanelComponent", () => {
  let component: JupyterNotebookPanelComponent;
  let fixture: ComponentFixture<JupyterNotebookPanelComponent>;

  let mockJupyterPanelService: any;
  let mockNotebookMigrationService: any;

  beforeEach(async () => {
    mockJupyterPanelService = {
      jupyterNotebookPanelVisible$: new Subject<boolean>(),
      setIframeRef: jasmine.createSpy("setIframeRef"),
      closeJupyterNotebookPanel: jasmine.createSpy("closeJupyterNotebookPanel"),
      minimizeJupyterNotebookPanel: jasmine.createSpy("minimizeJupyterNotebookPanel"),
    };

    mockNotebookMigrationService = {
      getJupyterIframeURL: jasmine
        .createSpy("getJupyterIframeURL")
        .and.returnValue(Promise.resolve("http://localhost:8888")),
    };

    await TestBed.configureTestingModule({
      declarations: [JupyterNotebookPanelComponent],
      imports: [BrowserModule],
      providers: [
        { provide: JupyterPanelService, useValue: mockJupyterPanelService },
        { provide: NotebookMigrationService, useValue: mockNotebookMigrationService },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JupyterNotebookPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    spyOn(component, "checkIframeRef").and.stub();
    expect(component).toBeTruthy();
  });

  it("should be hidden by default", () => {
    spyOn(component, "checkIframeRef").and.stub();
    expect(component.isVisible).toBeFalse();
  });

  it("should update visibility when service emits", async () => {
    spyOn(component, "checkIframeRef").and.stub();
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.isVisible).toBeTrue();
  });

  it("should fetch and sanitize URL when panel becomes visible", async () => {
    spyOn(component, "checkIframeRef").and.stub();
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(mockNotebookMigrationService.getJupyterIframeURL).toHaveBeenCalled();
    expect(component.jupyterUrl.toString()).toContain("http://localhost:8888");
  });

  it("should call setIframeRef when iframe exists and visible", done => {
    spyOn(component, "checkIframeRef").and.stub();
    component.isVisible = true;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();

    setTimeout(() => {
      expect(mockJupyterPanelService.setIframeRef).toHaveBeenCalledWith(mockIframe);
      done();
    }, 0);
  });

  it("should NOT call setIframeRef if not visible", done => {
    spyOn(component, "checkIframeRef").and.stub();
    component.isVisible = false;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();

    setTimeout(() => {
      expect(mockJupyterPanelService.setIframeRef).not.toHaveBeenCalled();
      done();
    }, 0);
  });

  it("should close panel via service", () => {
    spyOn(component, "checkIframeRef").and.stub();
    component.closePanel();
    expect(mockJupyterPanelService.closeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should minimize panel and update visibility", () => {
    spyOn(component, "checkIframeRef").and.stub();
    component.isVisible = true;

    component.minimizePanel();

    expect(component.isVisible).toBeFalse();
    expect(mockJupyterPanelService.minimizeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should clean up on destroy", () => {
    spyOn(component, "checkIframeRef").and.stub();
    const nextSpy = spyOn<any>(component["destroy$"], "next");
    const completeSpy = spyOn<any>(component["destroy$"], "complete");

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });
});
