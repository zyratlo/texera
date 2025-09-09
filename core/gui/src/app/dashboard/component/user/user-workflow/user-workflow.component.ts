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

import { AfterViewInit, Component, Input, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { NzModalService } from "ng-zorro-antd/modal";
import { firstValueFrom, from, lastValueFrom, Observable, of } from "rxjs";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { DashboardEntry, UserInfo } from "../../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowContent } from "../../../../common/type/workflow";
import { NzUploadFile } from "ng-zorro-antd/upload";
import * as JSZip from "jszip";
import { FiltersComponent } from "../filters/filters.component";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SearchService } from "../../../service/user/search.service";
import { SortMethod } from "../../../type/sort-method";
import { isDefined } from "../../../../common/util/predicate";
import { UserProjectService } from "../../../service/user/project/user-project.service";
import { map, mergeMap, switchMap, tap } from "rxjs/operators";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import { DownloadService } from "../../../service/user/download/download.service";
import { DASHBOARD_USER_WORKSPACE } from "../../../../app-routing.constant";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { JupyterUploadSuccessComponent } from "./notebook-migration-tool/notebook-migration.component";

/**
 * Saved-workflow-section component contains information and functionality
 * of the saved workflows section and is re-used in the user projects section when a project is clicked
 *
 * This component:
 *  - displays the workflows the user has access to
 *  - allows easy searching for workflows by name or other parameters using Fuse.js
 *  - sorting options
 *  - creation of a new workflow
 *  - AI generation of new workflow from jupyter notebook
 *
 * Steps to add new search parameter:
 *  1. Add a newly formatted dropdown menu in the html and css files, and a backend call to retrieve any necessary data
 *  2. Create an array of objects to hold data for the search parameter and a boolean "checked" variable
 *  3. Write a callback function that triggers when new dropdown menu changes and updates a "filtered" array of the selected options
 *  4. Add call to searchWorkflows() in this function
 *  5. Add parameter to buildMasterFilterList()
 *  6. Update synchronousSearch() to search based on the new parameter (either through filter iteration or fuse)
 *    - If it uses Fuse.js, create OrPathQuery object for multiple of the same new parameter and push it to the AndPathQuery array
 *    - Do this in asyncSearch(if it requires a backend call)
 *  7. Add parameter as key to searchCriteria
 *  8. If it uses Fuse.js, update fuse keys and searchCriteriaPathMapping
 *  9. Add parameter to updateDropdownMenus() and setDropdownSelectionsToUnchecked()
 *
 *
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-saved-workflow-section",
  templateUrl: "user-workflow.component.html",
  styleUrls: ["user-workflow.component.scss"],
})
export class UserWorkflowComponent implements AfterViewInit {
  private _searchResultsComponent?: SearchResultsComponent;
  public isLogin = this.userService.isLogin();
  private includePublic = false;
  public currentUid = this.userService.getCurrentUser()?.uid;
  @ViewChild(SearchResultsComponent) get searchResultsComponent(): SearchResultsComponent {
    if (this._searchResultsComponent) {
      return this._searchResultsComponent;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set searchResultsComponent(value: SearchResultsComponent) {
    this._searchResultsComponent = value;
  }
  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent) get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.search() });
    this._filters = value;
  }
  private masterFilterList: ReadonlyArray<string> | null = null;

  // receive input from parent components (UserProjectSection), if any
  @Input() public pid?: number = undefined;
  @Input() public accessLevel?: string = undefined;
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;

  constructor(
    private userService: UserService,
    private workflowPersistService: WorkflowPersistService,
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private modalService: NzModalService,
    private router: Router,
    private downloadService: DownloadService,
    private searchService: SearchService,
    private config: GuiConfigService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  public multiWorkflowsOperationButtonEnabled(): boolean {
    if (this._searchResultsComponent) {
      return this.searchResultsComponent?.entries.filter(i => i.checked).length > 0;
    } else {
      return false;
    }
  }

  public selectionTooltip: string = "Select all";

  public updateTooltip(): void {
    const entries = this.searchResultsComponent.entries;
    const allSelected = entries.every(entry => entry.checked);
    this.selectionTooltip = allSelected ? "Unselect all" : "Select all";
  }

  ngAfterViewInit() {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search());
  }

  /**
   * open the Modal to add workflow(s) to project
   */
  public onClickOpenAddWorkflow() {
    const modalRef = this.modalService.create({
      nzContent: NgbdModalAddProjectWorkflowComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Add Workflows To Project",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.search(true));
  }

  /**
   * open the Modal to remove workflow(s) from project
   */
  public onClickOpenRemoveWorkflow() {
    const modalRef = this.modalService.create({
      nzContent: NgbdModalRemoveProjectWorkflowComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Remove Workflows From Project",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.search(true));
  }

  /**
   * Searches workflows with keywords and filters given in the masterFilterList.
   * @returns
   */
  async search(forced: Boolean = false): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    let filterParams = this.filters.getSearchFilterParameters();
    if (isDefined(this.pid)) {
      // force the project id in the search query to be the current pid.
      filterParams.projectIds = [this.pid];
    }
    this.searchResultsComponent.reset((start, count) => {
      return firstValueFrom(
        this.searchService
          .executeSearch(
            this.filters.getSearchKeywords(),
            filterParams,
            start,
            count,
            "workflow",
            this.sortMethod,
            this.isLogin,
            this.includePublic
          )
          .pipe(map(({ entries, more }) => ({ entries, more })))
      );
    });
    await this.searchResultsComponent.loadMore();
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    const emptyWorkflowContent: WorkflowContent = {
      operators: [],
      commentBoxes: [],
      links: [],
      operatorPositions: {},
      settings: { dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize },
    };
    let localPid = this.pid;
    this.workflowPersistService
      .createWorkflow(emptyWorkflowContent, DEFAULT_WORKFLOW_NAME)
      .pipe(
        tap(createdWorkflow => {
          if (!createdWorkflow.workflow.wid) {
            throw new Error("Workflow creation failed.");
          }
        }),
        mergeMap(createdWorkflow => {
          // Check if localPid is defined; if so, add the workflow to the project
          if (localPid) {
            return this.userProjectService.addWorkflowToProject(localPid, createdWorkflow.workflow.wid!).pipe(
              // Regardless of the project addition outcome, pass the wid downstream
              map(() => createdWorkflow.workflow.wid)
            );
          } else {
            // If there's no localPid, skip adding to the project and directly pass the wid downstream
            return of(createdWorkflow.workflow.wid);
          }
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: (wid: number | undefined) => {
          // Use the wid here for navigation
          this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]).then(null);
        },
        error: (err: unknown) => this.notificationService.error("Workflow creation failed"),
      });
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   *
   * for workflow components inside a project-section, it will also add
   * the workflow to the project
   */
  public async onClickDuplicateWorkflow(entry: DashboardEntry): Promise<void> {
    if (entry.workflow.workflow.wid) {
      try {
        let duplicatedWorkflowsInfo: DashboardWorkflow[] = [];
        if (!isDefined(this.pid)) {
          duplicatedWorkflowsInfo = await firstValueFrom(
            this.workflowPersistService.duplicateWorkflow([entry.workflow.workflow.wid])
          );
        } else {
          const localPid = this.pid;
          duplicatedWorkflowsInfo = await firstValueFrom(
            this.workflowPersistService.duplicateWorkflow([entry.workflow.workflow.wid], localPid)
          );
        }

        const userIds = new Set<number>();
        duplicatedWorkflowsInfo.forEach(workflow => {
          if (workflow.ownerId) {
            userIds.add(workflow.ownerId);
          }
        });

        let userIdToInfoMap: { [key: number]: UserInfo } = {};
        if (userIds.size > 0) {
          userIdToInfoMap = await firstValueFrom(this.searchService.getUserInfo(Array.from(userIds)));
        }

        const newEntries = duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => {
          const entry = new DashboardEntry(duplicatedWorkflowInfo);
          const userInfo = userIdToInfoMap[duplicatedWorkflowInfo.ownerId];
          if (userInfo) {
            entry.setOwnerName(userInfo.userName);
            entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
          }
          return entry;
        });

        this.searchResultsComponent.entries = [...newEntries, ...this.searchResultsComponent.entries];
      } catch (err: unknown) {
        console.log("Error duplicating workflow:", err);
        // @ts-ignore // TODO: fix this with notification component
        alert((err as any).error);
      }
    }
  }

  /**
   * deleteWorkflow trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteWorkflow method in service which implements backend API.
   */

  public deleteWorkflow(entry: DashboardEntry): void {
    if (entry.workflow.workflow.wid == undefined) {
      return;
    }
    this.workflowPersistService
      .deleteWorkflow([entry.workflow.workflow.wid])
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          workflowEntry => workflowEntry.workflow.workflow.wid !== entry.workflow.workflow.wid
        );
      });
  }

  /**
   * Verify Uploaded file name and upload the file
   */
  public onClickUploadExistingWorkflowFromLocal = (file: NzUploadFile): Observable<boolean> => {
    const fileExtensionIndex = file.name.lastIndexOf(".");

    let upload$: Observable<void>;
    if (file.name.substring(fileExtensionIndex) === ".zip") {
      upload$ = this.handleZipUploads(file as unknown as Blob);
    } else {
      upload$ = this.handleFileUploads(file as unknown as Blob, file.name);
    }

    return upload$.pipe(
      switchMap(() => from(this.search(true))),
      tap(() => this.notificationService.success("Upload Successful")),
      switchMap(() => of(false))
    );
  };

  /**
   * process .zip file uploads
   */
  private handleZipUploads(zipFile: Blob): Observable<void> {
    let zip = new JSZip();
    return from(zip.loadAsync(zipFile)).pipe(
      switchMap(zip =>
        from(
          Promise.all(
            Object.keys(zip.files).map(relativePath =>
              zip.files[relativePath]
                .async("blob")
                .then(content => lastValueFrom(this.handleFileUploads(content, relativePath)))
            )
          )
        )
      ),
      map(() => undefined)
    );
  }

  /**
   * Process .json file uploads
   */
  private handleFileUploads(file: Blob, name: string): Observable<void> {
    return new Observable<void>(observer => {
      let reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        try {
          const result = reader.result;
          if (typeof result !== "string") {
            throw new Error("Incorrect format: file is not a string");
          }
          const workflowContent = JSON.parse(result) as WorkflowContent;
          const fileExtensionIndex = name.lastIndexOf(".");
          let workflowName = fileExtensionIndex === -1 ? name : name.substring(0, fileExtensionIndex);
          if (workflowName.trim() === "") {
            workflowName = DEFAULT_WORKFLOW_NAME;
          }
          this.workflowPersistService
            .createWorkflow(workflowContent, workflowName)
            .pipe(untilDestroyed(this))
            .subscribe({
              next: uploadedWorkflow => {
                this.searchResultsComponent.entries = [
                  ...this.searchResultsComponent.entries,
                  new DashboardEntry(uploadedWorkflow),
                ];
                observer.next();
                observer.complete();
              },
              error: (err: unknown) => {
                observer.error(err);
              },
            });
        } catch (error) {
          this.notificationService.error(
            "An error occurred when importing the workflow. Please import a workflow json file."
          );
          observer.error(error);
        }
      };
    });
  }

  /**
   * Download selected workflow as zip file
   */
  public onClickOpenDownloadZip(): void {
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
    if (checkedEntries.length === 0) {
      return;
    }

    const workflowEntries = checkedEntries.map(entry => ({
      id: entry.workflow.workflow.wid!,
      name: entry.workflow.workflow.name,
    }));

    this.downloadService
      .downloadWorkflowsAsZip(workflowEntries)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          // this.searchResultsComponent.clearAllSelections();
        },
        error: (err: unknown) => console.error("Error downloading workflows:", err),
      });
  }

  public onClickDuplicateSelectedWorkflows(): void {
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
    let targetWids: number[] = [];

    for (const entry of checkedEntries) {
      const wid = entry.workflow.workflow.wid;
      if (wid) {
        targetWids.push(wid);
      } else {
        return;
      }
    }

    if (targetWids.length > 0) {
      if (!isDefined(this.pid)) {
        this.workflowPersistService
          .duplicateWorkflow(targetWids)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];

              // this.searchResultsComponent.clearAllSelections();
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      } else {
        const localPid = this.pid;
        this.workflowPersistService
          .duplicateWorkflow(targetWids, localPid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];

              // this.searchResultsComponent.clearAllSelections();
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      }
    }
  }

  public handleConfirmDeleteSelectedWorkflows(): void {
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
    let targetWids: number[] = [];

    for (const entry of checkedEntries) {
      const wid = entry.workflow.workflow.wid;
      if (wid) {
        targetWids.push(wid);
      } else {
        return;
      }
    }

    if (targetWids.length > 0) {
      this.workflowPersistService
        .deleteWorkflow(targetWids)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: _ => {
            this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(workflowEntry => {
              let entryWid = workflowEntry.workflow.workflow.wid;
              // Check if wid is defined and if it's not included in targetWids
              return entryWid === undefined || !targetWids.includes(entryWid);
            });
          },
          // TODO: fix this with notification component
          error: (err: unknown) => alert(err),
        });
    }
  }

  /**
   * Resolve name conflict
   */
  private nameWorkflow(name: string, zip: JSZip) {
    let count = 0;
    let copyName = name;
    while (true) {
      if (!zip.files[copyName + ".json"]) {
        return copyName;
      } else {
        copyName = name + "-" + ++count;
      }
    }
  }

  public toggleSelection(): void {
    const allSelected = this.searchResultsComponent.entries.every(entry => entry.checked);
    if (allSelected) {
      this.searchResultsComponent.clearAllSelections();
      this.updateTooltip();
    } else {
      this.searchResultsComponent.selectAll();
      this.updateTooltip();
    }
  }

  public refreshSearchResult() {
    void this.search(true);
  }
}
