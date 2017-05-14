import {Component, ViewChild, OnInit} from '@angular/core';

import { CurrentDataService } from '../services/current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';
import {TableMetadata} from "../services/table-metadata";

declare var jQuery: any;
declare var Backbone: any;

declare var PrettyJSON: any;

@Component({
  moduleId: module.id,
  selector: 'side-bar-container',
  templateUrl: './side-bar.component.html',
  styleUrls: ['../style.css']
})

export class SideBarComponent {
  data: any;
  attributes: string[] = [];

  inSavedWindow = false;

  operatorId: number;
  operatorTitle: string;

  hiddenList: string[] = ["operatorType", "luceneAnalyzer", "matchingType", "spanListName"];

  selectorList: string[] = ["matchingType", "nlpEntityType", "splitType", "sampleType", "compareNumber", "aggregationType", "attributes", "tableName"].concat(this.hiddenList);

  matcherList: string[] = ["conjunction", "phrase", "substring"];
  nlpEntityList: string[] = ["noun", "verb", "adjective", "adverb", "ne_all", "number", "location", "person", "organization", "money", "percent", "date", "time"];
  regexSplitList: string[] = ["left", "right", "standalone"];
  samplerList: string[] = ["random", "firstk"];

  compareList: string[] = ["=", ">", ">=", "<", "<=", "!="];
  aggregationList: string[] = ["min", "max", "count", "sum", "average"];

  attributeItems:Array<string> = [];
  tableNameItems:Array<string> = [];
  selectedAttributesList:Array<string> = [];
  selectedAttribute:string = "";
  selectedTablesList:Array<string> = [];

  @ViewChild('MyModal')
  modal: ModalComponent;

  ModalOpen() {
    this.modal.open();
  }
  ModalClose() {
    this.modal.close();
  }

  checkInHidden(name: string) {
    return jQuery.inArray(name, this.hiddenList);
  }
  checkInSelector(name: string) {
    return jQuery.inArray(name, this.selectorList);
  }

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.inSavedWindow = false;
        this.data = data.operatorData;
        this.operatorId = data.operatorNum;
        this.operatorTitle = data.operatorData.properties.title;
        this.attributes = [];
        for (var attribute in data.operatorData.properties.attributes) {
          this.attributes.push(attribute);
        }
        this.selectedAttributesList = data.operatorData.properties.attributes.attributes;
        this.selectedAttribute = "-";
      });

    currentDataService.checkPressed$.subscribe(
      data => {
        console.log(data);
        this.inSavedWindow = false;

        if (data.code === 0) {
          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: JSON.parse(data.message)
          });
        } else {
          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: {"message": data.message}
          });
        }

        this.ModalOpen();

      });

    currentDataService.metadataRetrieved$.subscribe(
      data => {
        //TODO:: show attributes according to the source table.
        // Currently, it only shows attributes of promed table.
        let metadata: (Array<TableMetadata>) = data;
        metadata.forEach(x => {
          if (x.tableName === 'promed') {
            this.tableNameItems.push((x.tableName));
            x.attributes.forEach(
              y => {
                if (!y.attributeName.startsWith("_")) {
                  this.attributeItems.push(y.attributeName);
                }
              });
          }
        });
      }
    )
  }

  humanize(name: string): string {
    // TODO: rewrite this function to handle camel case
    // e.g.: humanize camelCase -> camel case
    var frags = name.split('_');
    for (var i = 0; i < frags.length; i++) {
      frags[i] = frags[i].charAt(0).toUpperCase() + frags[i].slice(1);
    }
    return frags.join(' ');
  }

  onSubmit() {
    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.inSavedWindow = true;
    jQuery('#the-flowchart').flowchart('setOperatorData', this.operatorId, this.data);
    this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
  }

  onDelete() {
    this.inSavedWindow = false;
    this.operatorTitle = "Operator";
    this.attributes = [];
    jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
    this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
  }

  selected (event:string) {
    this.selectedAttributesList.push(event);
  }

  selectedTable (event:string) {
    this.selectedTablesList.push(event);
  }

  manuallyAdded (event:string) {
    if (event.length === 0) {
      // removed all attributes
      this.selectedAttributesList = [];
    } else {
      this.selectedAttributesList = event.split(",");
    }
  }

  manuallyAddedTable (event:string) {
    if (event.length === 0) {
      // removed all tables
      this.selectedTablesList = [];
    } else {
      this.selectedTablesList = event.split(",");
    }
  }

}
