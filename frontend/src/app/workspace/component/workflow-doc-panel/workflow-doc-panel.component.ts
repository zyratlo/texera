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

import { ChangeDetectorRef, Component, inject, OnDestroy, OnInit } from "@angular/core";
import { DatePipe, NgFor, NgIf } from "@angular/common";
import { NZ_DRAWER_DATA } from "ng-zorro-antd/drawer";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { MarkdownService } from "ngx-markdown";
import { interval, Observable, Subscription } from "rxjs";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { DocEntry } from "../../service/workflow-doc/workflow-doc.service";

type DocPanelView = "intro" | "doc";

@Component({
  selector: "texera-workflow-doc-panel",
  templateUrl: "./workflow-doc-panel.component.html",
  styleUrls: ["./workflow-doc-panel.component.scss"],
  imports: [
    NgIf,
    NgFor,
    DatePipe,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTooltipDirective,
    NzWaveDirective,
  ],
})
export class WorkflowDocPanelComponent implements OnInit, OnDestroy {
  private modalData = inject(NZ_DRAWER_DATA, { optional: true });

  view: DocPanelView = "intro";
  renderedMarkdown = "";
  rawMarkdown = "";
  generatedAt: Date | null = null;
  history: DocEntry[] = [];
  isGenerating = false;
  copied = false;
  elapsedSeconds = 0;
  private generateSub?: Subscription;
  private elapsedTimerSub?: Subscription;

  constructor(
    private markdownService: MarkdownService,
    private notificationService: NotificationService,
    private workflowActionService: WorkflowActionService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.history = [...(this.modalData?.history ?? [])];
    const requestedView: DocPanelView | undefined = this.modalData?.initialView;
    if (requestedView === "doc" && this.history.length > 0) {
      this.loadEntry(this.history[0]);
      this.view = "doc";
    }
  }

  ngOnDestroy(): void {
    this.generateSub?.unsubscribe();
    this.elapsedTimerSub?.unsubscribe();
  }

  get elapsedDisplay(): string {
    const m = Math.floor(this.elapsedSeconds / 60);
    const s = this.elapsedSeconds % 60;
    return `${m}:${s.toString().padStart(2, "0")}`;
  }

  get hasHistory(): boolean {
    return this.history.length > 0;
  }

  viewEntry(entry: DocEntry): void {
    this.loadEntry(entry);
    this.setView("doc");
  }

  backToIntro(): void {
    if (this.isGenerating) return;
    this.setView("intro");
  }

  generate(): void {
    const onGenerate: (() => Observable<string>) | undefined = this.modalData?.onGenerate;
    if (!onGenerate || this.isGenerating) return;
    this.isGenerating = true;
    this.rawMarkdown = "";
    this.renderedMarkdown = "";
    this.generatedAt = null;
    this.setView("doc");
    this.startElapsedTimer();
    this.generateSub = onGenerate().subscribe({
      next: markdown => {
        const entry: DocEntry = { markdown, generatedAt: new Date() };
        this.history = [entry, ...this.history];
        this.isGenerating = false;
        this.stopElapsedTimer();
        this.loadEntry(entry);
        this.cdr.detectChanges();
      },
      error: (err: unknown) => {
        this.isGenerating = false;
        this.stopElapsedTimer();
        this.notificationService.error("Failed to generate documentation: " + (err as Error).message);
        if (!this.hasHistory) {
          this.setView("intro");
        } else {
          this.loadEntry(this.history[0]);
        }
        this.cdr.detectChanges();
      },
    });
  }

  private startElapsedTimer(): void {
    this.elapsedTimerSub?.unsubscribe();
    this.elapsedSeconds = 0;
    this.elapsedTimerSub = interval(1000).subscribe(() => {
      this.elapsedSeconds += 1;
      this.cdr.detectChanges();
    });
  }

  private stopElapsedTimer(): void {
    this.elapsedTimerSub?.unsubscribe();
    this.elapsedTimerSub = undefined;
  }

  onContentClick(event: MouseEvent): void {
    const anchor = (event.target as HTMLElement | null)?.closest("a") as HTMLAnchorElement | null;
    if (!anchor) return;
    const href = anchor.getAttribute("href") ?? "";
    const opRefPrefix = "texera:op:";
    if (!href.startsWith(opRefPrefix)) return;
    event.preventDefault();
    const operatorID = href.slice(opRefPrefix.length);
    if (!this.workflowActionService.getTexeraGraph().hasOperator(operatorID)) {
      this.notificationService.error(`Operator "${operatorID}" is not on the canvas (it may have been deleted or renamed since this report was generated)`);
      return;
    }
    this.workflowActionService.getTexeraGraph().triggerCenterOnOperatorEvent(operatorID);
  }

  copyToClipboard(): void {
    navigator.clipboard.writeText(this.rawMarkdown).then(() => {
      this.copied = true;
      setTimeout(() => (this.copied = false), 2000);
    });
  }

  isViewingEntry(entry: DocEntry): boolean {
    return this.generatedAt === entry.generatedAt;
  }

  private loadEntry(entry: DocEntry): void {
    this.rawMarkdown = entry.markdown;
    this.generatedAt = entry.generatedAt;
    this.renderMarkdown(entry.markdown);
  }

  private setView(next: DocPanelView): void {
    if (this.view === next) return;
    this.view = next;
    const onViewChange: ((view: DocPanelView) => void) | undefined = this.modalData?.onViewChange;
    onViewChange?.(next);
  }

  private renderMarkdown(md: string): void {
    if (md) {
      Promise.resolve(this.markdownService.parse(md)).then(html => {
        this.renderedMarkdown = html;
        this.cdr.detectChanges();
      });
    } else {
      this.renderedMarkdown = "";
    }
  }
}
