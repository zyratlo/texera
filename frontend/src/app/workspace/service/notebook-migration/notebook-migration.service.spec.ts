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
import { NotebookMigrationService } from "./notebook-migration.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NotificationService } from "src/app/common/service/notification/notification.service";

describe("NotebookMigrationService", () => {
  let service: NotebookMigrationService;
  let httpMock: HttpTestingController;
  let mockNotificationService: any;

  beforeEach(() => {
    mockNotificationService = {
      success: jasmine.createSpy("success"),
      error: jasmine.createSpy("error"),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        NotebookMigrationService,
        { provide: NotificationService, useValue: mockNotificationService },
      ],
    });

    service = TestBed.inject(NotebookMigrationService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  // getAvailableModels
  it("should fetch and map available models", (done) => {
    const mockResponse = {
      data: [
        { id: "gpt-4", object: "", created: 0, owned_by: "" },
        { id: "gpt-3.5", object: "", created: 0, owned_by: "" },
      ],
      object: "",
    };

    service.getAvailableModels().subscribe(models => {
      expect(models.length).toBe(2);
      expect(models[0].name).toBe("gpt-4");
      done();
    });

    const req = httpMock.expectOne(req => req.url.includes("/models"));
    expect(req.request.method).toBe("GET");
    req.flush(mockResponse);
  });

  it("should return empty array on getAvailableModels error", (done) => {
    service.getAvailableModels().subscribe(models => {
      expect(models).toEqual([]);
      done();
    });

    const req = httpMock.expectOne(req => req.url.includes("/models"));
    req.error(new ErrorEvent("Network error"));
  });

  // sendNotebookToJupyter
  it("should send notebook successfully and return 1", async () => {
    const mockNotebook: any = { cells: [] };

    const promise = service.sendNotebookToJupyter(mockNotebook);

    const req = httpMock.expectOne(req =>
      req.url.includes("/notebook-migration/set-notebook")
    );

    expect(req.request.method).toBe("POST");

    req.flush({ success: true });

    const result = await promise;

    expect(result).toBe(1);
    expect(mockNotificationService.success).toHaveBeenCalled();
  });

  it("should handle error when sending notebook and return 0", async () => {
    const mockNotebook: any = { cells: [] };

    const promise = service.sendNotebookToJupyter(mockNotebook);

    const req = httpMock.expectOne(req =>
      req.url.includes("/notebook-migration/set-notebook")
    );

    req.error(new ErrorEvent("Server error"));

    const result = await promise;

    expect(result).toBe(0);
    expect(mockNotificationService.error).toHaveBeenCalled();
  });

  // fetch methods
  it("should return Jupyter URL when fetch succeeds", async () => {
    spyOn(window, "fetch").and.resolveTo({
      ok: true,
      json: async () => ({ success: true, url: "http://jupyter" }),
    } as any);

    const result = await service.getJupyterURL();

    expect(result).toBe("http://jupyter");
  });

  it("should return null when fetch fails", async () => {
    spyOn(window, "fetch").and.resolveTo({
      ok: false,
      status: 500,
    } as any);

    const result = await service.getJupyterURL();

    expect(result).toBeNull();
  });

  it("should return iframe URL when fetch succeeds", async () => {
    spyOn(window, "fetch").and.resolveTo({
      ok: true,
      json: async () => ({ success: true, url: "http://iframe" }),
    } as any);

    const result = await service.getJupyterIframeURL();

    expect(result).toBe("http://iframe");
  });

  it("should return null when iframe fetch fails", async () => {
    spyOn(window, "fetch").and.resolveTo({
      ok: false,
      status: 500,
    } as any);

    const result = await service.getJupyterIframeURL();

    expect(result).toBeNull();
  });

  // mapping logic
  it("should set and get mapping", () => {
    const mockMapping: any = {
      cell_to_operator: { a: 1 },
      operator_to_cell: { b: 2 },
    };

    service.setMapping("test", mockMapping);

    expect(service.hasMapping("test")).toBeTrue();
    expect(service.getMapping("test")).toEqual(mockMapping);
  });

  it("should delete mapping", () => {
    service.setMapping("test", { cell_to_operator: {}, operator_to_cell: {} });

    service.deleteMapping("test");

    expect(service.hasMapping("test")).toBeFalse();
  });

  // storeNotebookAndMapping
  it("should call storeNotebookAndMapping API", () => {
    service.storeNotebookAndMapping(1, 1, {}, {}).subscribe();

    const req = httpMock.expectOne(req =>
      req.url.includes("/notebook-migration/store-notebook-and-mapping")
    );

    expect(req.request.method).toBe("POST");
  });
});
