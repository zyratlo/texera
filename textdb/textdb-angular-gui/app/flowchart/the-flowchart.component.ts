import { Component } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

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
    // console.log("Width now = " + this.TheFlowChartWidth);
    // console.log("Height now = " + this.TheFlowChartHeight);
    if (ZoomRatio < 1){
      ZoomRatio += 0.1;
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
    if (ZoomRatio >= 0.6){
      ZoomRatio -= 0.1;
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

  }
}
