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
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { firstValueFrom } from "rxjs";

describe("NotebookMigrationService", () => {
  let service: NotebookMigrationService;
  let httpMock: HttpTestingController;
  let mockNotificationService: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };
  // Mutable so individual describe blocks can flip the flag mid-spec by
  // reassigning `mockGuiConfigService.env.pythonNotebookMigrationEnabled`.
  // The service stores a reference to this object, so mutations are observed
  // on the next read of `this.enabled`.
  let mockGuiConfigService: { env: { pythonNotebookMigrationEnabled: boolean } };

  beforeEach(() => {
    mockNotificationService = {
      success: vi.fn(),
      error: vi.fn(),
    };
    mockGuiConfigService = { env: { pythonNotebookMigrationEnabled: true } };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        NotebookMigrationService,
        { provide: NotificationService, useValue: mockNotificationService },
        { provide: GuiConfigService, useValue: mockGuiConfigService },
      ],
    });

    service = TestBed.inject(NotebookMigrationService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  // getAvailableModels
  it("should fetch and map available models", async () => {
    const mockResponse = {
      data: [
        { id: "gpt-4", object: "", created: 0, owned_by: "" },
        { id: "gpt-3.5", object: "", created: 0, owned_by: "" },
      ],
      object: "",
    };

    const promise = firstValueFrom(service.getAvailableModels());

    const req = httpMock.expectOne(req => req.url.includes("/models"));
    expect(req.request.method).toBe("GET");
    req.flush(mockResponse);

    const models = await promise;
    expect(models.length).toBe(2);
    expect(models[0].name).toBe("gpt-4");
  });

  it("should return empty array on getAvailableModels error", async () => {
    const promise = firstValueFrom(service.getAvailableModels());

    const req = httpMock.expectOne(req => req.url.includes("/models"));
    req.error(new ErrorEvent("Network error"));

    expect(await promise).toEqual([]);
  });

  // sendNotebookToJupyter
  it("should send notebook successfully and return 1", async () => {
    const mockNotebook: any = { cells: [] };

    const promise = service.sendNotebookToJupyter(mockNotebook);

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/set-notebook"));

    expect(req.request.method).toBe("POST");

    req.flush({ success: true });

    const result = await promise;

    expect(result).toBe(1);
    expect(mockNotificationService.success).toHaveBeenCalled();
  });

  it("should handle error when sending notebook and return 0", async () => {
    const mockNotebook: any = { cells: [] };

    const promise = service.sendNotebookToJupyter(mockNotebook);

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/set-notebook"));

    req.error(new ErrorEvent("Server error"));

    const result = await promise;

    expect(result).toBe(0);
    expect(mockNotificationService.error).toHaveBeenCalled();
  });

  // jupyter URL methods (HttpClient so the JwtModule interceptor attaches the auth token)
  it("should return Jupyter URL when the request succeeds", async () => {
    const promise = service.getJupyterURL();

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/get-jupyter-url"));
    expect(req.request.method).toBe("GET");
    req.flush({ success: true, url: "http://jupyter" });

    expect(await promise).toBe("http://jupyter");
  });

  it("should return null when the Jupyter URL request fails", async () => {
    const promise = service.getJupyterURL();

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/get-jupyter-url"));
    req.flush({ success: false }, { status: 500, statusText: "Server Error" });

    expect(await promise).toBeNull();
  });

  it("should return iframe URL when the request succeeds", async () => {
    const promise = service.getJupyterIframeURL();

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/get-jupyter-iframe-url"));
    expect(req.request.method).toBe("GET");
    req.flush({ success: true, url: "http://iframe" });

    expect(await promise).toBe("http://iframe");
  });

  it("should return null when the iframe URL request fails", async () => {
    const promise = service.getJupyterIframeURL();

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/get-jupyter-iframe-url"));
    req.flush({ success: false }, { status: 500, statusText: "Server Error" });

    expect(await promise).toBeNull();
  });

  // mapping logic
  it("should set and get mapping", () => {
    const mockMapping: any = {
      cell_to_operator: { a: 1 },
      operator_to_cell: { b: 2 },
    };

    service.setMapping("test", mockMapping);

    expect(service.hasMapping("test")).toBe(true);
    expect(service.getMapping("test")).toEqual(mockMapping);
  });

  it("should delete mapping", () => {
    service.setMapping("test", { cell_to_operator: {}, operator_to_cell: {} });

    service.deleteMapping("test");

    expect(service.hasMapping("test")).toBe(false);
  });

  // storeNotebookAndMapping
  it("should call storeNotebookAndMapping API", () => {
    service.storeNotebookAndMapping(1, 1, {}, {}).subscribe();

    const req = httpMock.expectOne(req => req.url.includes("/notebook-migration/store-notebook-and-mapping"));

    expect(req.request.method).toBe("POST");
  });

  // Feature flag gate (defence in depth). With the flag off, every public
  // method must short-circuit — no HTTP traffic, no fetch, no LLM lifecycle,
  // no notifications.
  describe("when the feature flag is disabled", () => {
    beforeEach(() => {
      mockGuiConfigService.env.pythonNotebookMigrationEnabled = false;
    });

    it("getAvailableModels emits an empty array and makes no HTTP call", async () => {
      const models = await firstValueFrom(service.getAvailableModels());
      expect(models).toEqual([]);
      httpMock.expectNone(req => req.url.includes("/models"));
    });

    it("sendToAIGenerateWorkflow rejects with a disabled-feature error", async () => {
      await expect(service.sendToAIGenerateWorkflow({ cells: [] } as any, "gpt-4")).rejects.toThrow(/disabled/i);
    });

    it("sendNotebookToJupyter returns 0 with no HTTP call or notification", async () => {
      const result = await service.sendNotebookToJupyter({ cells: [] } as any);
      expect(result).toBe(0);
      expect(mockNotificationService.success).not.toHaveBeenCalled();
      expect(mockNotificationService.error).not.toHaveBeenCalled();
      httpMock.expectNone(req => req.url.includes("/notebook-migration/set-notebook"));
    });

    it("getJupyterURL returns null without making an HTTP call", async () => {
      const result = await service.getJupyterURL();
      expect(result).toBeNull();
      httpMock.expectNone(req => req.url.includes("/notebook-migration/get-jupyter-url"));
    });

    it("getJupyterIframeURL returns null without making an HTTP call", async () => {
      const result = await service.getJupyterIframeURL();
      expect(result).toBeNull();
      httpMock.expectNone(req => req.url.includes("/notebook-migration/get-jupyter-iframe-url"));
    });

    it("storeNotebookAndMapping emits without making an HTTP call", async () => {
      await firstValueFrom(service.storeNotebookAndMapping(1, 1, {}, {}));
      httpMock.expectNone(req => req.url.includes("/notebook-migration/store-notebook-and-mapping"));
    });
  });
});
