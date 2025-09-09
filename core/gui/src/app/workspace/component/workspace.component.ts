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

import { Location } from "@angular/common";
import { AfterViewInit, Component, HostListener, OnDestroy, OnInit, ViewChild, ViewContainerRef } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { UserService } from "../../common/service/user/user.service";
import { WorkflowPersistService } from "../../common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "../../common/type/workflow";
import { OperatorMetadataService } from "../service/operator-metadata/operator-metadata.service";
import { UndoRedoService } from "../service/undo-redo/undo-redo.service";
import { WorkflowCacheService } from "../service/workflow-cache/workflow-cache.service";
import { WorkflowActionService } from "../service/workflow-graph/model/workflow-action.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { debounceTime, distinctUntilChanged, filter, switchMap, throttleTime } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { of } from "rxjs";
import { isDefined } from "../../common/util/predicate";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { WorkflowConsoleService } from "../service/workflow-console/workflow-console.service";
import { OperatorReuseCacheStatusService } from "../service/workflow-status/operator-reuse-cache-status.service";
import { CodeEditorService } from "../service/code-editor/code-editor.service";
import { WorkflowMetadata } from "src/app/dashboard/type/workflow-metadata.interface";
import { EntityType, HubService } from "../../hub/service/hub.service";
import { THROTTLE_TIME_MS } from "../../hub/component/workflow/detail/hub-workflow-detail.component";
import { WorkflowCompilingService } from "../service/compile-workflow/workflow-compiling.service";
import { DASHBOARD_USER_WORKSPACE } from "../../app-routing.constant";
import { GuiConfigService } from "../../common/service/gui-config.service";
import { checkIfWorkflowBroken } from "../../common/util/workflow-check";

export const SAVE_DEBOUNCE_TIME_IN_MS = 5000;

@UntilDestroy()
@Component({
  selector: "texera-workspace",
  templateUrl: "./workspace.component.html",
  styleUrls: ["./workspace.component.scss"],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
  ],
})
export class WorkspaceComponent implements AfterViewInit, OnInit, OnDestroy {
  public pid?: number = undefined;
  public writeAccess: boolean = false;
  public isLoading: boolean = false;
  // variable for track whether we are waiting for AI to finish generating (whether a loading icon should show)
  public isWaitingForOpenAi = false;
  @ViewChild("codeEditor", { read: ViewContainerRef }) codeEditorViewRef!: ViewContainerRef;

  /**
   * Flag to ensure auto persist is registered only once.  This prevents multiple
   * subscriptions and avoids accidental persistence of an empty workflow
   * before the actual workflow is loaded from backend.
   */
  private autoPersistRegistered = false;

  constructor(
    private userService: UserService,
    // list additional 3 services in constructor so they are initialized even if no one use them directly
    // TODO: make their lifecycle better
    private workflowCompilingService: WorkflowCompilingService,
    private workflowConsoleService: WorkflowConsoleService,
    private operatorReuseCacheStatusService: OperatorReuseCacheStatusService,
    // end of additional services
    private undoRedoService: UndoRedoService,
    private workflowCacheService: WorkflowCacheService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private location: Location,
    private route: ActivatedRoute,
    private operatorMetadataService: OperatorMetadataService,
    private message: NzMessageService,
    private router: Router,
    private notificationService: NotificationService,
    private hubService: HubService,
    private codeEditorService: CodeEditorService,
    private config: GuiConfigService
  ) {}

  ngOnInit() {
    /**
     * On initialization of the workspace, there are two possibilities regarding which component has
     * routed to this component:
     *
     * 1. Routed to this component from within UserProjectSection component
     *    - track the pid identifying that project
     *    - upon persisting of a workflow, must also ensure it is also added to the project
     *
     * 2. Routed to this component from SavedWorkflowSection component
     *    - there is no related project, parseInt will return NaN.
     *    - NaN || undefined will result in undefined.
     */
    this.pid = parseInt(this.route.snapshot.queryParams.pid) || undefined;
    this.workflowActionService.setHighlightingEnabled(true);
  }

  ngAfterViewInit(): void {
    /**
     * On initialization of the workspace, there could be three cases:
     *
     * - with userSystem enabled, usually during prod mode:
     * 1. Accessed by URL `/`, no workflow is in the URL (Cold Start):
     -    - A new `WorkflowActionService.DEFAULT_WORKFLOW` is created, which is an empty workflow with undefined id.
     *    - After an Auto-persist being triggered by a WorkflowAction event, it will create a new workflow in the database
     *    and update the URL with its new ID from database.
     * 2. Accessed by URL `/workflow/:id` (refresh manually, or redirected from dashboard workflow list):
     *    - It will retrieve the workflow from database with the given ID. Because it has an ID, it will be linked to the database
     *    - Auto-persist will be triggered upon all workspace events.
     *
     * - with userSystem disabled, during dev mode:
     * 1. Accessed by URL `/`, with a workflow cached (refresh manually):
     *    - This will trigger the WorkflowCacheService to load the workflow from cache.
     *    - Auto-cache will be triggered upon all workspace events.
     *
     * WorkflowActionService is the single source of the workflow representation. Both WorkflowCacheService and WorkflowPersistService are
     * reflecting changes from WorkflowActionService.
     */
    // clear the current workspace, reset as `WorkflowActionService.DEFAULT_WORKFLOW`
    this.workflowActionService.resetAsNewWorkflow();

    if (this.config.env.userSystemEnabled) {
      // if a workflow id is present in the route, display loading spinner immediately while loading
      const widInRoute = this.route.snapshot.params.id;
      if (widInRoute) {
        this.isLoading = true;
        this.workflowActionService.disableWorkflowModification();
      }

      this.onWIDChange();
      this.updateViewCount();
    }

    this.registerLoadOperatorMetadata();
    this.codeEditorService.vc = this.codeEditorViewRef;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy() {
    if (this.userService.isLogin() && this.workflowPersistService.isWorkflowPersistEnabled()) {
      const workflow = this.workflowActionService.getWorkflow();
      this.workflowPersistService.persistWorkflow(workflow).pipe(untilDestroyed(this)).subscribe();
    }

    this.codeEditorViewRef.clear();
    this.workflowActionService.clearWorkflow();
  }

  registerAutoCacheWorkFlow(): void {
    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowCacheService.setCacheWorkflow(this.workflowActionService.getWorkflow());
      });
  }

  registerAutoPersistWorkflow(): void {
    // make sure it is only registered once
    if (this.autoPersistRegistered) {
      return;
    }
    this.autoPersistRegistered = true;

    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin() && this.workflowPersistService.isWorkflowPersistEnabled()) {
          this.workflowPersistService
            .persistWorkflow(this.workflowActionService.getWorkflow())
            .pipe(untilDestroyed(this))
            .subscribe((updatedWorkflow: Workflow) => {
              if (this.workflowActionService.getWorkflowMetadata().wid !== updatedWorkflow.wid) {
                this.location.go(`${DASHBOARD_USER_WORKSPACE}/${updatedWorkflow.wid}`);
              }
              this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
            });
          // to sync up with the updated information, such as workflow.wid
        }
      });
  }

  loadWorkflowWithId(wid: number): void {
    // disable the workspace until the workflow is fetched from the backend
    this.isLoading = true;
    this.workflowActionService.disableWorkflowModification();
    this.workflowPersistService
      .retrieveWorkflow(wid)
      .pipe(untilDestroyed(this))
      .subscribe(
        (workflow: Workflow) => {
          if (checkIfWorkflowBroken(workflow)) {
            this.notificationService.error(
              "Sorry! The workflow is broken and cannot be persisted. Please contact the system admin."
            );
          }

          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          // remember URL fragment
          const fragment = this.route.snapshot.fragment;
          // load the fetched workflow
          this.workflowActionService.reloadWorkflow(workflow);
          this.workflowActionService.enableWorkflowModification();
          // set the URL fragment to previous value
          // because reloadWorkflow will highlight/unhighlight all elements
          // which will change the URL fragment
          this.router.navigate([], {
            relativeTo: this.route,
            fragment: fragment !== null ? fragment : undefined,
            preserveFragment: false,
          });
          // highlight the operator, comment box, or link in the URL fragment
          if (fragment) {
            if (this.workflowActionService.getTexeraGraph().hasElementWithID(fragment)) {
              this.workflowActionService.highlightElements(false, fragment);
            } else {
              this.notificationService.error(`Element ${fragment} doesn't exist`);
              // remove the fragment from the URL
              this.router.navigate([], { relativeTo: this.route });
            }
          }
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
          this.isLoading = false;
          this.registerAutoPersistWorkflow();
          this.triggerCenter();
        },
        () => {
          this.workflowActionService.resetAsNewWorkflow();
          // enable workspace for modification
          this.workflowActionService.enableWorkflowModification();
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
          this.message.error("You don't have access to this workflow, please log in with an appropriate account");
          this.isLoading = false;
        }
      );
  }

  registerLoadOperatorMetadata() {
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        let wid = this.route.snapshot.params.id;
        if (this.config.env.userSystemEnabled) {
          // load workflow with wid if presented in the URL
          if (wid) {
            // show loading spinner right away while waiting for workflow to load
            this.isLoading = true;
            // temporarily disable modification to prevent editing an empty workflow before real data is loaded
            this.workflowActionService.disableWorkflowModification();
            // if wid is present in the url, load it from the backend once the user info is ready
            this.userService
              .userChanged()
              .pipe(untilDestroyed(this))
              .subscribe(() => {
                this.loadWorkflowWithId(wid);
              });
          } else {
            // no workflow to load; directly register auto persist for brand-new workflow
            this.registerAutoPersistWorkflow();
          }
        } else {
          // remember URL fragment
          const fragment = this.route.snapshot.fragment;
          // fetch the cached workflow first
          const cachedWorkflow = this.workflowCacheService.getCachedWorkflow();
          // responsible for saving the existing workflow in cache
          this.registerAutoCacheWorkFlow();
          // load the cached workflow
          this.workflowActionService.reloadWorkflow(cachedWorkflow);
          // set the URL fragment to previous value
          // because reloadWorkflow will highlight/unhighlight all elements
          // which will change the URL fragment
          this.router.navigate([], {
            relativeTo: this.route,
            fragment: fragment !== null ? fragment : undefined,
            preserveFragment: false,
          });
          // highlight the operator, comment box, or link in the URL fragment
          if (fragment) {
            if (this.workflowActionService.getTexeraGraph().hasElementWithID(fragment)) {
              this.workflowActionService.highlightElements(false, fragment);
            } else {
              this.notificationService.error(`Element ${fragment} doesn't exist`);
              // remove the fragment from the URL
              this.router.navigate([], { relativeTo: this.route });
            }
          }
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
        }
      });
  }

  onWIDChange() {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(
        switchMap(() => of(this.workflowActionService.getWorkflowMetadata())),
        filter((metadata: WorkflowMetadata) => isDefined(metadata.wid)),
        distinctUntilChanged()
      )
      .pipe(untilDestroyed(this))
      .subscribe((metadata: WorkflowMetadata) => {
        this.writeAccess = !metadata.readonly;
      });
  }

  updateViewCount() {
    let wid = this.route.snapshot.params.id;
    let uid = this.userService.getCurrentUser()?.uid;
    this.hubService
      .postView(wid, uid ? uid : 0, EntityType.Workflow)
      .pipe(throttleTime(THROTTLE_TIME_MS))
      .pipe(untilDestroyed(this))
      .subscribe();
  }

  public triggerCenter(): void {
    this.workflowActionService.getTexeraGraph().triggerCenterEvent();
  }
}
