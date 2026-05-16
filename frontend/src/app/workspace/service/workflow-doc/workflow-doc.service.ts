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
  id: string;
  markdown: string;
  generatedAt: Date;
  edited?: boolean;
  written?: boolean;
  title?: string;
}

export type DocPanelView = "intro" | "doc";

export interface DocEditingState {
  entryId: string | null;
  content: string;
}

interface PersistedShape {
  history: Record<string, DocEntry[]>;
  lastView: Record<string, DocPanelView>;
  editing: Record<string, DocEditingState>;
}

const STORAGE_KEY = "texera.workflowDoc.v1";

@Injectable({
  providedIn: "root",
})
export class WorkflowDocService {
  private readonly AGENT_API_BASE = "/api";
  private history = new Map<number | undefined, DocEntry[]>();
  private lastViews = new Map<number | undefined, DocPanelView>();
  private editingStates = new Map<number | undefined, DocEditingState>();

  constructor(
    private http: HttpClient,
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService
  ) {
    this.loadFromStorage();
  }

  getHistory(wid: number | undefined): readonly DocEntry[] {
    return this.history.get(wid) ?? [];
  }

  getLastView(wid: number | undefined): DocPanelView | null {
    return this.lastViews.get(wid) ?? null;
  }

  setLastView(wid: number | undefined, view: DocPanelView): void {
    this.lastViews.set(wid, view);
    this.writeAll();
  }

  getEditingState(wid: number | undefined): DocEditingState | null {
    return this.editingStates.get(wid) ?? null;
  }

  setEditingState(wid: number | undefined, state: DocEditingState | null): void {
    if (state) {
      this.editingStates.set(wid, state);
    } else {
      this.editingStates.delete(wid);
    }
    this.writeAll();
  }

  deleteHistoryEntry(wid: number | undefined, entry: DocEntry): void {
    const list = this.history.get(wid) ?? [];
    this.history.set(wid, list.filter(e => e !== entry));
    const editing = this.editingStates.get(wid);
    if (editing && editing.entryId === entry.id) {
      this.editingStates.delete(wid);
    }
    this.writeAll();
  }

  createBlankEntry(wid: number | undefined, markdown: string): DocEntry {
    const entry: DocEntry = { id: this.generateId(), markdown, generatedAt: new Date(), written: true };
    const list = [entry, ...(this.history.get(wid) ?? [])];
    this.history.set(wid, list);
    this.writeAll();
    return entry;
  }

  renameEntry(wid: number | undefined, entry: DocEntry, newTitle: string): void {
    const list = this.history.get(wid) ?? [];
    const target = list.find(e => e === entry);
    if (target) {
      const trimmed = newTitle.trim();
      if (trimmed) {
        target.title = trimmed;
      } else {
        delete target.title;
      }
      this.writeAll();
    }
  }

  duplicateEntry(wid: number | undefined, source: DocEntry): DocEntry {
    const entry: DocEntry = {
      id: this.generateId(),
      markdown: source.markdown,
      generatedAt: new Date(),
      edited: source.edited,
      written: source.written,
      title: source.title ? `${source.title} (copy)` : undefined,
    };
    const list = [entry, ...(this.history.get(wid) ?? [])];
    this.history.set(wid, list);
    this.writeAll();
    return entry;
  }

  updateHistoryEntry(wid: number | undefined, entry: DocEntry, newMarkdown: string): Date {
    const list = this.history.get(wid) ?? [];
    const target = list.find(e => e === entry);
    const newTimestamp = new Date();
    if (target) {
      target.markdown = newMarkdown;
      target.generatedAt = newTimestamp;
      target.edited = true;
      const reordered = [target, ...list.filter(e => e !== target)];
      this.history.set(wid, reordered);
    }
    this.writeAll();
    return newTimestamp;
  }

  generateDocumentation(): Observable<DocEntry> {
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
              const entry: DocEntry = {
                id: this.generateId(),
                markdown: response.markdown,
                generatedAt: new Date(),
              };
              const list = [...(this.history.get(wid) ?? [])];
              list.unshift(entry);
              this.history.set(wid, list);
              this.writeAll();
              return entry;
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

  private generateId(): string {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID();
    }
    return `doc-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  private widFromKey(key: string): number | undefined {
    return key === "undefined" ? undefined : Number(key);
  }

  private widToKey(wid: number | undefined): string {
    return wid === undefined ? "undefined" : String(wid);
  }

  private loadFromStorage(): void {
    if (typeof localStorage === "undefined") return;
    let raw: string | null;
    try {
      raw = localStorage.getItem(STORAGE_KEY);
    } catch {
      return;
    }
    if (!raw) return;
    let parsed: PersistedShape;
    try {
      parsed = JSON.parse(raw) as PersistedShape;
    } catch {
      return;
    }
    if (parsed.history) {
      for (const [key, entries] of Object.entries(parsed.history)) {
        const wid = this.widFromKey(key);
        const revived = entries.map(e => ({ ...e, generatedAt: new Date(e.generatedAt) }));
        this.history.set(wid, revived);
      }
    }
    if (parsed.lastView) {
      for (const [key, view] of Object.entries(parsed.lastView)) {
        this.lastViews.set(this.widFromKey(key), view);
      }
    }
    if (parsed.editing) {
      for (const [key, state] of Object.entries(parsed.editing)) {
        this.editingStates.set(this.widFromKey(key), state);
      }
    }
  }

  private writeAll(): void {
    if (typeof localStorage === "undefined") return;
    const payload: PersistedShape = { history: {}, lastView: {}, editing: {} };
    this.history.forEach((entries, wid) => {
      payload.history[this.widToKey(wid)] = entries;
    });
    this.lastViews.forEach((view, wid) => {
      payload.lastView[this.widToKey(wid)] = view;
    });
    this.editingStates.forEach((state, wid) => {
      payload.editing[this.widToKey(wid)] = state;
    });
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch {
      // Quota exceeded or storage unavailable — degrade silently to in-memory.
    }
  }
}
