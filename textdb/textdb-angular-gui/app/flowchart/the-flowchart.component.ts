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
		</div>
	`,
  styleUrls: ['../style.css'],
})

export class TheFlowchartComponent {

  TheOperatorNumNow: number;
  TheFlowChartWidth : number;
  TheFlowChartHeight : number;
  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.TheOperatorNumNow = data.operatorNum;
      }
    );
  }

  zoomInDiv(){
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    if (ZoomRatio < MAX_SCALE){
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
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    if (ZoomRatio >= MIN_SCALE + INCREMENT){
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
    });


    jQuery('html').keyup(function(e) { //key binding function
      if (e.keyCode === 8) { //backspace
        var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
        if (current_id !== null) {
          jQuery('#the-flowchart').flowchart('deleteSelected');
          current.currentDataService.clearData();
          current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
          console.log("HELLO");
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
      }
    });

    this.TheFlowChartWidth = parseInt(jQuery("#the-flowchart").css("width"));
    this.TheFlowChartHeight = parseInt(jQuery("#the-flowchart").css("height"));

    this.initializePanzoom(jQuery('#the-flowchart').parent(), this.TheFlowChartWidth, this.TheFlowChartHeight);

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
      e.preventDefault();
      var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
      var zoomOut = delta;
      // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
      currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut / 40 - 1))));
      jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoom]);
      jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoom], {
        animate: false,
        focal: e
      });
      var ZoomRatio = possibleZooms[currentZoom];
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

    });
    // panzoom end
  }
  
}
