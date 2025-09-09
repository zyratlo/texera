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

import { AfterViewInit, Component, HostListener, OnDestroy, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { MAIN_CANVAS } from "../workflow-editor.component";
import * as joint from "jointjs";
import { JointGraphWrapper } from "../../../service/workflow-graph/model/joint-graph-wrapper";
import { PanelService } from "../../../service/panel/panel.service";
import { CdkDrag } from "@angular/cdk/drag-drop";
import { JupyterPanelService } from "../../../service/jupyter-panel/jupyter-panel.service";

@UntilDestroy()
@Component({
  selector: "texera-mini-map",
  templateUrl: "mini-map.component.html",
  styleUrls: ["mini-map.component.scss"],
})
export class MiniMapComponent implements AfterViewInit, OnDestroy {
  @ViewChild("navigatorDrag", { static: false }) navigatorDrag!: CdkDrag;

  scale = 0;
  paper!: joint.dia.Paper;
  dragging = false;
  hidden = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private panelService: PanelService,
    private jupyterPanelService: JupyterPanelService
  ) {}

  ngAfterViewInit() {
    const map = document.getElementById("mini-map")!;
    this.scale = map.offsetWidth / (MAIN_CANVAS.xMax - MAIN_CANVAS.xMin);
    new joint.dia.Paper({
      el: map,
      model: this.workflowActionService.getJointGraphWrapper().jointGraph,
      background: { color: "#F6F6F6" },
      interactive: false,
      width: map.offsetWidth,
      height: map.offsetHeight,
    })
      .scale(this.scale)
      .translate(-MAIN_CANVAS.xMin * this.scale, -MAIN_CANVAS.yMin * this.scale);
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaperAttachedStream()
      .pipe(untilDestroyed(this))
      .subscribe(mainPaper => {
        this.paper = mainPaper;
        this.updateNavigator();
        mainPaper.on("translate", () => this.updateNavigator());
        mainPaper.on("scale", () => this.updateNavigator());
        mainPaper.on("resize", () => this.updateNavigator());
      });
    this.hidden = JSON.parse(localStorage.getItem("mini-map") as string) || false;

    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => (this.hidden = true));
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => (this.hidden = false));
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("mini-map", JSON.stringify(this.hidden));
  }

  onDrag(event: any) {
    this.paper.translate(
      this.paper.translate().tx + -event.event.movementX / this.scale,
      this.paper.translate().ty + -event.event.movementY / this.scale
    );
  }

  private updateNavigator(): void {
    if (!this.dragging) {
      const editor = document.getElementById("workflow-editor")!;
      const navigator = document.getElementById("mini-map-navigator")!;
      const editorRect = editor.getBoundingClientRect();

      const point = this.paper.pageToLocalPoint({
        x: editorRect.left,
        y: editorRect.top,
      });

      navigator.style.transform = "";
      navigator.style.left = (point.x - MAIN_CANVAS.xMin) * this.scale + "px";
      navigator.style.top = (point.y - MAIN_CANVAS.yMin) * this.scale + "px";
      navigator.style.width = (editor.offsetWidth / this.paper.scale().sx) * this.scale + "px";
      navigator.style.height = (editor.offsetHeight / this.paper.scale().sy) * this.scale + "px";
    }
  }

  public onClickZoomOut(): void {
    // if zoom is already at minimum, don't zoom out again.
    if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMin()) {
      return;
    }

    // make the ratio small.
    this.workflowActionService
      .getJointGraphWrapper()
      .setZoomProperty(
        this.workflowActionService.getJointGraphWrapper().getZoomRatio() - JointGraphWrapper.ZOOM_CLICK_DIFF
      );
  }

  /**
   * This method will increase the zoom ratio and send the new zoom ratio value
   *  to the joint graph wrapper to change overall zoom ratio that is used in
   *  zoom buttons and mouse wheel zoom.
   *
   * If the zoom ratio already reaches maximum, this method will not do anything.
   */
  public onClickZoomIn(): void {
    // if zoom is already reach maximum, don't zoom in again.
    if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMax()) {
      return;
    }

    // make the ratio big.
    this.workflowActionService
      .getJointGraphWrapper()
      .setZoomProperty(
        this.workflowActionService.getJointGraphWrapper().getZoomRatio() + JointGraphWrapper.ZOOM_CLICK_DIFF
      );
  }

  /**
   * This method will expand and redisplay the jupyter notebook.
   */
  public onClickExpandJupyterNotebookPanel(): void {
    this.jupyterPanelService.openJupyterNotebookPanel();
  }

  public triggerCenter(): void {
    this.workflowActionService.getTexeraGraph().triggerCenterEvent();
    if (this.navigatorDrag) this.navigatorDrag.reset();
  }
}
