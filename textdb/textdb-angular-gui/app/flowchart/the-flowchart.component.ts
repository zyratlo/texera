import { Component } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

@Component({
  moduleId: module.id,
  selector: 'flowchart-container',
  template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
		</div>
	`,
  styleUrls: ['../style.css'],
})

export class TheFlowchartComponent {

  TheOperatorNumNow: number;

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.TheOperatorNumNow = data.operatorNum;
      }
    );
  }


  initialize(data: any) {
    var current = this;

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
  }
}
