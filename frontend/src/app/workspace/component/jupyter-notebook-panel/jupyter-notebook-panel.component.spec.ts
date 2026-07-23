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
      setIframeRef: vi.fn(),
      closeJupyterNotebookPanel: vi.fn(),
      minimizeJupyterNotebookPanel: vi.fn(),
    };

    mockNotebookMigrationService = {
      getJupyterIframeURL: vi.fn().mockResolvedValue("http://localhost:8888"),
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
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    expect(component).toBeTruthy();
  });

  it("should be hidden by default", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    expect(component.isVisible).toBe(false);
  });

  it("should update visibility when service emits", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.isVisible).toBe(true);
  });

  it("should fetch and sanitize URL when panel becomes visible", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(mockNotebookMigrationService.getJupyterIframeURL).toHaveBeenCalled();
    expect(component.jupyterUrl.toString()).toContain("http://localhost:8888");
  });

  it("should call setIframeRef when iframe exists and visible", async () => {
    component.isVisible = true;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();

    await new Promise<void>(resolve => setTimeout(resolve, 0));
    expect(mockJupyterPanelService.setIframeRef).toHaveBeenCalledWith(mockIframe);
  });

  it("should NOT call setIframeRef if not visible", async () => {
    component.isVisible = false;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();

    await new Promise<void>(resolve => setTimeout(resolve, 0));
    expect(mockJupyterPanelService.setIframeRef).not.toHaveBeenCalled();
  });

  it("should close panel via service", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.closePanel();
    expect(mockJupyterPanelService.closeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should minimize panel and update visibility", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.isVisible = true;

    component.minimizePanel();

    expect(component.isVisible).toBe(false);
    expect(mockJupyterPanelService.minimizeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should clean up on destroy", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    const nextSpy = vi.spyOn(component["destroy$"] as any, "next");
    const completeSpy = vi.spyOn(component["destroy$"] as any, "complete");

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });
});
