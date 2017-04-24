import { Component, ViewChild } from '@angular/core';

import { MockDataService } from './services/mock-data-service';
import { CurrentDataService } from './services/current-data-service';

import { TheFlowchartComponent } from './flowchart/the-flowchart.component';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: 'my-app',
    template: `
		<div id="wrapper">
		    <flowchart-container class="container fill" #theFlowchart></flowchart-container>
		</div>
	`,
    providers: [MockDataService, CurrentDataService],
    styleUrls: ['style.css']
})
export class AppComponent {
	name = 'Angular';

    constructor(private currentDataService: CurrentDataService, private mockDataService: MockDataService) { }

    @ViewChild('theFlowchart') theFlowchart: TheFlowchartComponent;

    ngAfterViewInit() {
        var current = this;
        
        jQuery(document).ready(function() {
            current.theFlowchart.initialize({});

        });

    }
}
