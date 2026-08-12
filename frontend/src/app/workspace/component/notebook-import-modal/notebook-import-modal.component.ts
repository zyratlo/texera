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

import { Component, HostListener, inject, OnDestroy } from "@angular/core";
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzUploadComponent, NzUploadFile } from "ng-zorro-antd/upload";
import { Observable } from "rxjs";
import { AsyncPipe, NgIf, NgFor, NgOptimizedImage } from "@angular/common";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";

// Passed in via nzData. The modal hands the selected file and model to the opener, which runs the
// generation and persistence. The modal shows a loading state while this promise is pending.
// Resolve true to close the modal (generation succeeded and the opener navigated away), false to
// keep it open with the user's selection intact (bad file or a failure the user can retry).
export interface NotebookImportModalData {
  requestImport: (file: NzUploadFile, model: string) => Promise<boolean>;
}

/**
 * The "AI Generate Workflow from Python Notebook" modal body. It owns the upload form and
 * the three model-dropdown states (loading / has models / none). On Submit it hands the file
 * and model to its opener via the requestImport callback (passed in through nzData) and shows a
 * loading state while generation runs, closing itself only when that resolves true. Mirrors the
 * component-as-nzContent pattern used by the other modals opened from the menu.
 */
@Component({
  selector: "texera-notebook-import-modal",
  templateUrl: "./notebook-import-modal.component.html",
  styleUrls: ["./notebook-import-modal.component.scss"],
  imports: [
    NgIf,
    NgFor,
    AsyncPipe,
    NgOptimizedImage,
    ReactiveFormsModule,
    NzFormModule,
    NzSelectModule,
    NzSpinComponent,
    NzUploadComponent,
    NzButtonComponent,
    NzIconDirective,
  ],
})
export class NotebookImportModalComponent implements OnDestroy {
  private readonly fb = inject(FormBuilder);
  private readonly modalRef = inject(NzModalRef);
  private readonly notebookMigrationService = inject(NotebookMigrationService);
  private readonly data: NotebookImportModalData = inject(NZ_MODAL_DATA);

  public readonly importForm: FormGroup = this.fb.group({
    file: [null, Validators.required],
    model: ["", Validators.required],
  });

  // Drives the three model-dropdown states: pending (loading), a non-empty list (selectable),
  // and an empty list (no models available, e.g. the fetch failed or the feature is off).
  public readonly models$: Observable<{ name: string }[]> = this.notebookMigrationService.getAvailableModels();

  public beforeUpload = (file: NzUploadFile) => {
    this.importForm.patchValue({ file });
    this.importForm.get("file")?.markAsDirty();
    this.importForm.get("file")?.updateValueAndValidity();
    return false; // prevent auto upload
  };

  public onCancel(): void {
    this.modalRef.close();
  }

  // True while generation is running. Guards against a second submit and drives the loading
  // spinner shown over the form.
  public isSubmitting = false;
  private startTime: number | null = null;
  private timerHandle: ReturnType<typeof setInterval> | null = null;

  public ngOnDestroy(): void {
    this.stopTimer();
  }

  public get formattedElapsedTime(): string {
    const diffMs = this.startTime === null ? 0 : Date.now() - this.startTime;
    const totalSeconds = Math.floor(diffMs / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  }

  // Empty on purpose: with default change detection, the visibilitychange event firing in the
  // Angular zone is itself what triggers a repaint, so the stopwatch catches up immediately when
  // the user returns to a backgrounded tab. The handler body is irrelevant.
  @HostListener("document:visibilitychange")
  public onVisibilityChange(): void {}

  private startTimer(): void {
    this.stopTimer();
    this.startTime = Date.now();
    // The interval body is intentionally empty: the elapsed value is computed from startTime, and
    // with default change detection the zone-patched timer firing is what repaints it each second.
    this.timerHandle = setInterval(() => {}, 1000);
  }

  private stopTimer(): void {
    if (this.timerHandle !== null) {
      clearInterval(this.timerHandle);
      this.timerHandle = null;
    }
  }

  public async onSubmit(): Promise<void> {
    if (this.isSubmitting || !this.importForm.valid) return;
    const file: NzUploadFile = this.importForm.get("file")?.value;
    const model: string = this.importForm.get("model")?.value;
    this.isSubmitting = true;
    this.startTimer();
    this.modalRef.updateConfig({ nzClosable: false, nzMaskClosable: false, nzKeyboard: false });
    try {
      // Run generation via the opener; close only if it succeeds, so a failure leaves this modal
      // open with the selection preserved.
      if (await this.data.requestImport(file, model)) {
        this.modalRef.close();
        return;
      }
      this.modalRef.updateConfig({ nzClosable: true, nzMaskClosable: true, nzKeyboard: true });
    } finally {
      this.isSubmitting = false;
      this.stopTimer();
    }
  }
}
