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

import { Component, inject, OnInit } from "@angular/core";
import { NgIf } from "@angular/common";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { MarkdownService } from "ngx-markdown";

@Component({
  selector: "texera-workflow-doc-panel",
  templateUrl: "./workflow-doc-panel.component.html",
  styleUrls: ["./workflow-doc-panel.component.scss"],
  imports: [
    NgIf,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTooltipDirective,
    NzWaveDirective,
  ],
})
export class WorkflowDocPanelComponent implements OnInit {
  private modalData = inject(NZ_MODAL_DATA, { optional: true });

  renderedMarkdown = "";
  copied = false;
  rawMarkdown = "";

  constructor(private markdownService: MarkdownService) {}

  ngOnInit(): void {
    this.rawMarkdown = this.modalData?.markdown ?? "";
    if (this.rawMarkdown) {
      Promise.resolve(this.markdownService.parse(this.rawMarkdown)).then(html => {
        this.renderedMarkdown = html;
      });
    }
  }

  copyToClipboard(): void {
    navigator.clipboard.writeText(this.rawMarkdown).then(() => {
      this.copied = true;
      setTimeout(() => (this.copied = false), 2000);
    });
  }
}
