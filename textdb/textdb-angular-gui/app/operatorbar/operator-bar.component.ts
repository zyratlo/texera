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
    this.initializeOperators(container);
    this.initializeDrop(this.theDropDownNow);
  }

  initializeDrop (currentDrop : string) {
    jQuery('html').mouseup(function(e){
      jQuery('.dropdown-content').css({
        "display" : "none",
      });
      var checkOnClickIsDropDown = jQuery('.dropdown');
      var checkOnIcon = jQuery('.group-icon');
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
