import { Component,ViewChild, OnInit } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';

declare var jQuery: any;
declare var Backbone: any;
declare var PrettyJSON: any;
var previousOpenHeight: number = 300;

@Component({
    moduleId: module.id,
    selector: 'result-container',
    templateUrl: './result-bar.component.html',
    styleUrls: ['../style.css'],
})
export class ResultBarComponent {
  result: any;
  attribute: string[] = [];
  previousResultHandleTop: number = -5;
  checkErrorOrDetail: number = 0;

  @ViewChild('ResultModal')
  modal: ModalComponent;

  ModalOpen() {
    this.modal.open();
  }
  ModalClose() {
    this.modal.close();
  }
  constructor (private currentDataService: CurrentDataService){
    currentDataService.checkPressed$.subscribe(
      data => {
        // stop the loading animation of the run button
        jQuery('.navigation-btn').button('reset');
        this.attribute = [];
        // check if the result is valid
        if (data.code === 0) {
          this.result = JSON.parse(data.message);
          console.log(this.result);
          for (var each in this.result[0]){
            this.attribute.push(each);
          }
          // open the result bar automatically
          this.openResultBar();
          jQuery(".result-bar-title").css({"background-color" : "#4d79ff"});
          jQuery(".result-bar-title").hover(
            function() {jQuery(".result-bar-title").css({"background-color" : "#1a53ff"})},
            function() {jQuery(".result-bar-title").css({"background-color" : "#4d79ff"})});
          jQuery( ".result-bar-title" ).html( "<b>Result Window</b>");
        } else {
          // pop the modal when not valid
            this.checkErrorOrDetail = 0;
            var node = new PrettyJSON.view.Node({
              el: jQuery("#ResultElem"),
              data: {"message": data.message}
            });
            this.ModalOpen();
            this.closeResultBar();
            jQuery(".result-bar-title").css({
              "background-color" : "#ff3333"
            });
            jQuery(".result-bar-title").hover(
              function() {jQuery(".result-bar-title").css({"background-color" : "#ff0000"})},
              function() {jQuery(".result-bar-title").css({"background-color" : "#ff3333"})});
            jQuery(".result-bar-title").html( "<b>Error</b>");
        }
      }
    );
  }

  shortenString(longText: string){
    longText = longText.toString();
    if (longText.length < 25) {
      return longText;
    } else {
      return longText.substring(0,25) + " ...";
    }
  }
  checkIfObject(eachAttribute: string){
    return typeof eachAttribute === "object";
  }
  resultBarClicked(){
    // check if the result bar is opened or closed
    var currentResultBarStatus = jQuery('#result-table-bar').css('display');
    if (currentResultBarStatus === "none"){
      this.openResultBar()
    } else {
      this.closeResultBar()
    }
  }

  openResultBar(){
    jQuery("#result-table-bar").css({
      "display":"block",
      "height" : previousOpenHeight + "px",
    });
    var new_container_height = previousOpenHeight + 40;
    jQuery('#ngrip').css({"top":"-5px"});
    jQuery("#flow-chart-container").css({"height":"calc(100% - " + new_container_height + "px)"});
    this.redrawDraggable();
  }

  closeResultBar(){
    jQuery("#result-table-bar").css({
      "display":"none",
      "height" : "0px",
    });
    jQuery('#ngrip').css({"top":"-5px"});
    jQuery("#flow-chart-container").css({"height":"calc(100% - 40px)"});
    this.redrawDraggable();
  }

  redrawDraggable(){
    this.previousResultHandleTop = -parseInt(jQuery('#result-table-bar').css('height'), 10) - 5;
    jQuery("#ngrip").draggable( "destroy" );
    this.initializeResizing(this.previousResultHandleTop);
  }


  displayRowDetail(singleResult: any){
    this.checkErrorOrDetail = 1;
    var node = new PrettyJSON.view.Node({
      el: jQuery("#ResultElem"),
      data: singleResult
    });
    this.ModalOpen();
  }

  // initialized the default draggable / resizable result bar
  initializing(){
    this.initializeResizing(this.previousResultHandleTop);
  }

  initializeResizing(previousHeight: number){
    jQuery("#ngrip").draggable({
      axis:"y",
      containment: "window",
      drag: function( event, ui ) {
        // calculate the position
        var endPosition = previousHeight + 5 + ui.position.top;
        // if endPosition exceeds the maximum
        if (endPosition < -305){
          ui.position.top = -305;
          jQuery("#result-table-bar").css({
            "display":"block",
            "height" : "300px",
          });
          jQuery("#flow-chart-container").css({"height":"calc(100% - 340px)"});
        } else if (endPosition > -5){
          // if endPosition is lower than the minimum
          ui.position.top = -5;
          jQuery("#result-table-bar").css({
            "display":"none",
            "height" : "0px",
          });
          jQuery("#flow-chart-container").css({"height":"calc(100% - 40px)"});
        } else {
          var new_height = -endPosition - 5; // include the drag button
          var new_height2 = new_height + 40; // include the drag button and the title bar

          // redraw 2 fields to resize
          jQuery("#flow-chart-container").css({"height":"calc(100% - " + new_height2 + "px)"});
          jQuery("#result-table-bar").css({
            "display":"block",
            "height" : new_height + "px",
          });
        }
      },

      stop: function( event, ui ) {
        // get the current result bar height
        var newResultBarHeight = parseInt(jQuery('#result-table-bar').css('height'), 10);
        var ResultBarDisplay = jQuery('#result-table-bar').css('display');
        // if at minimum
        if (newResultBarHeight === 0 || ResultBarDisplay === "none"){
          previousHeight = -5;
          previousOpenHeight = 300; //restore default
        } else if (newResultBarHeight === 300){
          previousHeight = -305;
          previousOpenHeight = newResultBarHeight;
        } else {
          // previous height is used for calculating the movement of the result bar and flowchart
          previousHeight = previousHeight + 5 + parseInt(jQuery('#ngrip').css('top'), 10);
          previousOpenHeight = newResultBarHeight;
        }
        // make sure drag button is directly above the result bar
        jQuery('#ngrip').css({"top":"-5px"});

      }
    });

  }

}
