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
import { DatePipe, NgFor, NgIf } from "@angular/common";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { diffLines, diffWordsWithSpace } from "diff";
import { DocEntry } from "../../service/workflow-doc/workflow-doc.service";

interface DiffSegment {
  text: string;
  changed: boolean;
}

interface DiffRow {
  leftSegments: DiffSegment[] | null;
  rightSegments: DiffSegment[] | null;
  leftType: "eq" | "del" | "pad";
  rightType: "eq" | "add" | "pad";
}

@Component({
  selector: "texera-workflow-doc-diff",
  templateUrl: "./workflow-doc-diff.component.html",
  styleUrls: ["./workflow-doc-diff.component.scss"],
  imports: [NgFor, NgIf, DatePipe],
})
export class WorkflowDocDiffComponent implements OnInit {
  private modalData = inject(NZ_MODAL_DATA, { optional: true }) as {
    base: DocEntry;
    head: DocEntry;
  } | null;

  base: DocEntry | null = null;
  head: DocEntry | null = null;
  rows: DiffRow[] = [];
  addedCount = 0;
  removedCount = 0;

  ngOnInit(): void {
    if (!this.modalData) return;
    this.base = this.modalData.base;
    this.head = this.modalData.head;
    this.computeDiff();
  }

  private computeDiff(): void {
    if (!this.base || !this.head) return;
    const parts = diffLines(this.base.markdown, this.head.markdown);
    const rows: DiffRow[] = [];
    let added = 0;
    let removed = 0;

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const lines = part.value.replace(/\n$/, "").split("\n");
      if (part.added) {
        for (const line of lines) {
          rows.push({
            leftSegments: null,
            rightSegments: [{ text: line, changed: true }],
            leftType: "pad",
            rightType: "add",
          });
          added++;
        }
      } else if (part.removed) {
        const next = parts[i + 1];
        if (next?.added) {
          const nextLines = next.value.replace(/\n$/, "").split("\n");
          const pairCount = Math.min(lines.length, nextLines.length);
          for (let j = 0; j < pairCount; j++) {
            const { left, right } = this.diffLinePair(lines[j], nextLines[j]);
            rows.push({
              leftSegments: left,
              rightSegments: right,
              leftType: "del",
              rightType: "add",
            });
            removed++;
            added++;
          }
          for (let j = pairCount; j < lines.length; j++) {
            rows.push({
              leftSegments: [{ text: lines[j], changed: true }],
              rightSegments: null,
              leftType: "del",
              rightType: "pad",
            });
            removed++;
          }
          for (let j = pairCount; j < nextLines.length; j++) {
            rows.push({
              leftSegments: null,
              rightSegments: [{ text: nextLines[j], changed: true }],
              leftType: "pad",
              rightType: "add",
            });
            added++;
          }
          i++;
        } else {
          for (const line of lines) {
            rows.push({
              leftSegments: [{ text: line, changed: true }],
              rightSegments: null,
              leftType: "del",
              rightType: "pad",
            });
            removed++;
          }
        }
      } else {
        for (const line of lines) {
          rows.push({
            leftSegments: [{ text: line, changed: false }],
            rightSegments: [{ text: line, changed: false }],
            leftType: "eq",
            rightType: "eq",
          });
        }
      }
    }

    this.rows = rows;
    this.addedCount = added;
    this.removedCount = removed;
  }

  private diffLinePair(leftLine: string, rightLine: string): {
    left: DiffSegment[];
    right: DiffSegment[];
  } {
    const wordParts = diffWordsWithSpace(leftLine, rightLine);
    const left: DiffSegment[] = [];
    const right: DiffSegment[] = [];
    for (const wp of wordParts) {
      if (wp.added) {
        right.push({ text: wp.value, changed: true });
      } else if (wp.removed) {
        left.push({ text: wp.value, changed: true });
      } else {
        left.push({ text: wp.value, changed: false });
        right.push({ text: wp.value, changed: false });
      }
    }
    return { left, right };
  }
}
