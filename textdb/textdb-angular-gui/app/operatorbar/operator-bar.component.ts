import { Component } from '@angular/core';

import { MockDataService } from '../services/mock-data-service';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

@Component({
  moduleId: module.id,
  selector: '[operator-bar]',
  templateUrl: './operator-bar.component.html',
  styleUrls: ['../style.css']
})
export class OperatorBarComponent {

  theDropDownNow : string;

  constructor(private mockDataService: MockDataService, private currentDataService: CurrentDataService) { }

  initialize() {
    var container = jQuery('#the-flowchart').parent();
    var InitialWidth = parseInt(jQuery("#the-flowchart").css("width"));
    var InitialHeight = parseInt(jQuery("#the-flowchart").css("height"));

    this.initializePanzoom(container, InitialWidth, InitialHeight);
    this.initializeOperators(container);
    this.initializeDrop(this.theDropDownNow);
  }

  initializeDrop (currentDrop : string) {
    jQuery('html').mouseup(function(e){
      jQuery('.dropdown-content').css({
        "display" : "none",
      });
      var checkOnClickIsDropDown = jQuery('.dropdown');
      var checkOnIcon = jQuery('.fa');
      if (!checkOnClickIsDropDown.is(e.target) && !checkOnIcon.is(e.target)){
        currentDrop = "";
      } else {
        if (checkOnIcon.is(e.target)){
          var currentDropType = jQuery(e.target).parent().data('dropdown-type');
        } else {
          var currentDropType = jQuery(e.target).data('dropdown-type');
        }
        if (currentDropType !== currentDrop){
          var dropdownID = "#" + currentDropType;
          jQuery(dropdownID).css({
            "display" : "block",
          });
          currentDrop = currentDropType;
        } else {
          currentDrop = "";
        }
      }
    });
  }

  initializePanzoom(container: any, InitialWidth: number, InitialHeight: number) {
    // Panzoom initialization...
    jQuery('#the-flowchart').panzoom({
      disablePan: true, // disable the pan
      // contain : true, // if pan, only can pan within flowchart div
      minScale: 0.5,
      maxScale: 1,
      increment: 0.1,
    });
    var possibleZooms = [0.5, 0.6, 0.7, 0.8, 0.9, 1];
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


  initializeOperators(container: any) {
    var findOperatorData = function(opeartorId: number, opeatorList: [any]): any {
      for (let operator of opeatorList) {
        if (operator.id === opeartorId) {
          return operator.jsonData;
        }
      }
      return null;
    }


    let operatorList;
    this.mockDataService.getOperatorList().then(
      data => {
        operatorList = data;
      },
      error => {
        console.log(error);
      }
    );

    var draggableOperators = jQuery('.draggable_operator');
    draggableOperators.draggable({
      cursor: "move",
      opacity: 0.7,

      appendTo: 'body',
      zIndex: 1000,

      helper: function(e) {
        var dragged = jQuery(this);
        var operatorId = parseInt(dragged.data('matcher-type'));
        var operatorData = findOperatorData(operatorId, operatorList);

        return jQuery('#the-flowchart').flowchart('getOperatorElement', operatorData);
      },

      stop: function(e, ui) {
        var dragged = jQuery(this);

        var operatorId = parseInt(dragged.data('matcher-type'));
        var operatorData = findOperatorData(operatorId, operatorList);

        var newData = {
          top: 0,
          left: 20,
          properties: operatorData.properties
        }

        var elOffset = ui.offset;
        var containerOffset = container.offset();

        var positionRatio = jQuery('#the-flowchart').flowchart('getPositionRatio');

        console.log("CONTAINoffset left " + containerOffset.left);
        console.log("CONTAINoffset top " + containerOffset.top);
        console.log("elOffset left = " + elOffset.left * positionRatio);
        console.log("elOffset top = " + elOffset.top * positionRatio);

        if (elOffset.left  > containerOffset.left &&
          elOffset.top > containerOffset.top &&
          elOffset.left  < containerOffset.left + container.width() &&
          elOffset.top < containerOffset.top + container.height()) {

          var flowchartOffset = jQuery('#the-flowchart').offset();

          var relativeLeft = elOffset.left - flowchartOffset.left;
          var relativeTop = elOffset.top - flowchartOffset.top;

          relativeLeft /= positionRatio;
          relativeTop /= positionRatio;

          newData.left = relativeLeft;
          newData.top = relativeTop;

          var operatorNum = jQuery('#the-flowchart').flowchart('addOperator', newData);

          jQuery('#the-flowchart').flowchart('selectOperator', operatorNum); // select the created operator
        }
      }
    });
  }
}
