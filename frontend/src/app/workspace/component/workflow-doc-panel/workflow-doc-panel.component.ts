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

import { Component, inject, OnDestroy, OnInit } from "@angular/core";
import { DatePipe, NgIf } from "@angular/common";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { MarkdownService } from "ngx-markdown";
import { Observable, Subscription } from "rxjs";
import { NotificationService } from "../../../common/service/notification/notification.service";

type DocPanelView = "intro" | "doc";

@Component({
  selector: "texera-workflow-doc-panel",
  templateUrl: "./workflow-doc-panel.component.html",
  styleUrls: ["./workflow-doc-panel.component.scss"],
  imports: [
    NgIf,
    DatePipe,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTooltipDirective,
    NzWaveDirective,
  ],
})
export class WorkflowDocPanelComponent implements OnInit, OnDestroy {
  private modalData = inject(NZ_MODAL_DATA, { optional: true });

  view: DocPanelView = "intro";
  renderedMarkdown = "";
  rawMarkdown = "";
  generatedAt: Date | null = null;
  isGenerating = false;
  copied = false;
  private generateSub?: Subscription;

  constructor(
    private markdownService: MarkdownService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.rawMarkdown = this.modalData?.cachedMarkdown ?? "";
    this.generatedAt = this.modalData?.cachedGeneratedAt ?? null;
    if (this.rawMarkdown) {
      this.renderMarkdown(this.rawMarkdown);
    }
  }

  ngOnDestroy(): void {
    this.generateSub?.unsubscribe();
  }

  get hasCached(): boolean {
    return this.rawMarkdown.length > 0;
  }

  viewLatest(): void {
    if (this.hasCached) {
      this.view = "doc";
    }
  }

  backToIntro(): void {
    if (this.isGenerating) return;
    this.view = "intro";
  }

  generate(): void {
    const onGenerate: (() => Observable<string>) | undefined = this.modalData?.onGenerate;
    if (!onGenerate || this.isGenerating) return;
    this.isGenerating = true;
    this.view = "doc";
    this.generateSub = onGenerate().subscribe({
      next: markdown => {
        this.rawMarkdown = markdown;
        this.generatedAt = new Date();
        this.renderMarkdown(markdown);
        this.isGenerating = false;
      },
      error: (err: unknown) => {
        this.isGenerating = false;
        this.notificationService.error("Failed to generate documentation: " + (err as Error).message);
        if (!this.hasCached) {
          this.view = "intro";
        }
      },
    });
  }

  copyToClipboard(): void {
    navigator.clipboard.writeText(this.rawMarkdown).then(() => {
      this.copied = true;
      setTimeout(() => (this.copied = false), 2000);
    });
  }

  private renderMarkdown(md: string): void {
    if (md) {
      Promise.resolve(this.markdownService.parse(md)).then(html => {
        this.renderedMarkdown = html;
      });
    } else {
      this.renderedMarkdown = "";
    }
  }
}
