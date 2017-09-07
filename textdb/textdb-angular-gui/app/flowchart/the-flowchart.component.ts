import { Component } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

const MIN_SCALE = 0.5;
const MAX_SCALE = 1;
const INCREMENT = 0.1;

@Component({
  moduleId: module.id,
  selector: 'flowchart-container',
  template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
      <button class="zoomInButton" (click)="zoomInDiv()"> + </button>
      <button class="zoomOutButton" (click)="zoomOutDiv()"> - </button>
      <button class="btn btn-default navbar-btn excelDownloadButton" (click)="downloadExcel()" disabled><i class="fa fa-file-excel-o excelIcon" aria-hidden="true"></i>Download As Excel</button>
    </div>
	`,
  styleUrls: ['../style.css'],
})

export class TheFlowchartComponent {

  TheOperatorNumNow: number;
  TheFlowChartWidth : number;
  TheFlowChartHeight : number;
  currentResult: any;

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.TheOperatorNumNow = data.operatorNum;
      }
    );
    currentDataService.checkPressed$.subscribe(
      // used for download as excel button
      data => {
        if (data.code === 0) {
          this.currentResult = JSON.parse(data.message);
          jQuery('.excelDownloadButton').prop("disabled",false);
          jQuery('.excelDownloadButton').css({"opacity":"1"});
        } else {
          jQuery('.excelDownloadButton').prop("disabled",true);
          jQuery('.excelDownloadButton').css({"opacity":"0.5"});
        }
      }
    );
  }

  downloadExcel() {
    // do nothing now
    // need to implement backend download excel functions
  }


  zoomInDiv(){
    // hide menu when zoomed
    jQuery("#menu").css({
      "display" : "none",
    });
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    if (ZoomRatio < MAX_SCALE){
      jQuery('#the-flowchart').flowchart('zoomCalled');
      ZoomRatio += INCREMENT;
      jQuery('#the-flowchart').flowchart('setPositionRatio', ZoomRatio);
      jQuery('#the-flowchart').panzoom('zoom', ZoomRatio, {
        animate: false,
      });
      var new_width = this.TheFlowChartWidth / ZoomRatio;
      var left_side_add = (new_width - this.TheFlowChartWidth) / 2 ;
      var new_height = this.TheFlowChartHeight / ZoomRatio;
      var top_side_add = (new_height - this.TheFlowChartHeight) / 2;
      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
    }
  }

  zoomOutDiv(){
    // hide menu when zoomed
    jQuery("#menu").css({
      "display" : "none",
    });
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    if (ZoomRatio >= MIN_SCALE + INCREMENT){
      jQuery('#the-flowchart').flowchart('zoomCalled');
      ZoomRatio -= INCREMENT;
      jQuery('#the-flowchart').flowchart('setPositionRatio', ZoomRatio);
      jQuery('#the-flowchart').panzoom('zoom', ZoomRatio, {
        animate: false,
      });
      var new_width = this.TheFlowChartWidth / ZoomRatio;
      var left_side_add = (new_width - this.TheFlowChartWidth) / 2 ;
      var new_height = this.TheFlowChartHeight / ZoomRatio;
      var top_side_add = (new_height - this.TheFlowChartHeight) / 2;
      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
    }
  }


  initialize(data: any) {
    var current = this;

    // unselect operator when user click other div
    jQuery('html').mousedown(function(e) {
      var container = jQuery(".form-control");
      if (container.is(e.target)) {
        jQuery("#the-flowchart").flowchart("unselectOperator");
      }
      // hide the right click menu when click other modules
      var flowchart_container = jQuery("#the-flowchart");
      if (flowchart_container.has(e.target).length <= 0){
      jQuery("#the-flowchart").flowchart("hideRightClickMenu");
      }
    });


    jQuery('html').keyup(function(e) { //key binding function
      if (e.keyCode === 8) { //backspace
        var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
        if (current_id !== null) {
          jQuery('#the-flowchart').flowchart('deleteSelected');
          current.currentDataService.clearData();
          current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        }
      } else if (e.keyCode === 46) { //delete
        var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
        if (current_id !== null) {
          jQuery('#the-flowchart').flowchart('deleteSelected');
          current.currentDataService.clearData();
          current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        }
      }
    });

    jQuery('#the-flowchart').flowchart({
      data: data,
      multipleLinksOnOutput: true,
      onOperatorSelect: function(operatorId) {
        current.currentDataService.selectData(operatorId);
        return true;
      },
      onOperatorUnselect: function(operatorId) {
        return true;
      },
      // called when the delete button on the right click menu is clicked
      onRightClickedDelete : function (operatorId) {
        current.currentDataService.clearData();
        current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        return true;
      }
    });

    this.TheFlowChartWidth = parseInt(jQuery("#the-flowchart").css("width"));
    this.TheFlowChartHeight = parseInt(jQuery("#the-flowchart").css("height"));

    var defaultWidth =  this.TheFlowChartWidth;
    var defaultHeight = this.TheFlowChartHeight;

    var intializeMethod = this.initializePanzoom;
    intializeMethod(jQuery('#the-flowchart').parent(), this.TheFlowChartWidth, this.TheFlowChartHeight);



    var handleResize = function(){
      var currentWindowWidth = jQuery(window).width();
      var currentWindowHeight = jQuery(window).height();
      var currentSideBarWidth = jQuery("#sidebar-wrapper").width();

      var new_width = currentWindowWidth - currentSideBarWidth - 10;
      var new_height = currentWindowHeight - 100;
      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : 0 + "px",
        "height" : new_height - 100 + "px",
        "top" : 0 + "px",
      });
      jQuery('#the-flowchart').panzoom("reset");
      jQuery('#the-flowchart').panzoom("destroy");
      intializeMethod(jQuery('#the-flowchart').parent(), new_width, new_height);
    }

    window.onresize = handleResize;

  }

  initializePanzoom(container: any, InitialWidth: number, InitialHeight: number) {
    // Panzoom initialization...
    jQuery('#the-flowchart').panzoom({
      disablePan: true, // disable the pan
      // contain : true, // if pan, only can pan within flowchart div
      minScale: MIN_SCALE,
      maxScale: MAX_SCALE,
      increment: INCREMENT,
    });
    var possibleZooms = [];
    for (var i = MIN_SCALE; i < MAX_SCALE; i += INCREMENT) {
      possibleZooms.push(i);
    }
    var currentZoom = 2;
    container.on('mousewheel.focal', function(e) {
      jQuery('#the-flowchart').flowchart('zoomCalled');
      e.preventDefault();
      var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
      var zoomOut = delta;
      // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
      currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut / 40 - 1))));
      var currentZoomRound = Math.round(currentZoom);
      jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoomRound]);
      jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoomRound], {
        animate: false,
        focal: e
      });
      var ZoomRatio = possibleZooms[currentZoomRound];
      // enlarge the div ratio so there's more space for the operators
      var new_width = InitialWidth / ZoomRatio;
      var left_side_add = (new_width - InitialWidth) / 2 ;

      var new_height = InitialHeight / ZoomRatio;
      var top_side_add = (new_height - InitialHeight) / 2;

      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
      // hide menu when zoomed
      jQuery("#menu").css({
        "display" : "none",
      });
    });
    // panzoom end
  }

}
