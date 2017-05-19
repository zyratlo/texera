import {Component, ViewChild, OnInit} from '@angular/core';
import { FileUploader, FileDropDirective } from 'ng2-file-upload/ng2-file-upload';
import {FileItem} from "ng2-file-upload/file-upload/file-item.class";
import {ParsedResponseHeaders} from "ng2-file-upload/file-upload/file-uploader.class";

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

  operatorId: number;
  operatorTitle: string;

  hiddenList: string[] = ["operatorType", "luceneAnalyzer", "matchingType", "spanListName"];

  selectorList: string[] = ["matchingType", "nlpEntityType", "splitType", "sampleType", "compareNumber", "aggregationType", "attributes", "tableName", "attribute"].concat(this.hiddenList);

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
  metadataList:Array<TableMetadata> = [];

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

  checkOperatorNameIsUploadDict() {
    return this.operatorId === 21;
  }

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.data = data.operatorData;
        this.operatorId = data.operatorNum;
        this.operatorTitle = data.operatorData.properties.title;
        this.attributes = [];
        for (var attribute in data.operatorData.properties.attributes) {
          this.attributes.push(attribute);
        }

        // initialize selected attributes
        this.selectedAttribute = "";

        // and load previously saved attributes and proper attributes for the selected table
        this.selectedAttributesList = data.operatorData.properties.attributes.attributes;
        this.getAttributesForTable(data.operatorData.properties.attributes.tableName);
      });

    currentDataService.checkPressed$.subscribe(
      data => {
        jQuery.hideLoading();
        console.log(data);
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
        this.metadataList = data;
        let metadata: (Array<TableMetadata>) = data;
        metadata.forEach(x => {
          this.tableNameItems.push((x.tableName));
        });
      }
    );

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

  onFormChange (attribute: string) {
    jQuery("#the-flowchart").flowchart("setOperatorData", this.operatorId, this.data);
  }

  onDelete() {
    this.operatorTitle = "Operator";
    this.attributes = [];
    jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
    this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
  }

  attributeSelected () {
    this.selectedAttributesList.push(this.selectedAttribute);
    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.onFormChange("attributes");
  }

  manuallyAdded (event:string) {
    if (event.length === 0) {
      // removed all attributes
      this.selectedAttributesList = [];
    } else {
      this.selectedAttributesList = event.split(",");
    }

    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.onFormChange("attributes");
  }

  getAttributesForTable (event:string) {
    if (!event) {
      return;
    }

    this.attributeItems = [];

    this.metadataList.forEach(x => {
      if (x.tableName === event) {
        x.attributes.forEach(
          y => {
            if (!y.attributeName.startsWith("_")) {
              this.attributeItems.push(y.attributeName);
            }
          });
      }
    });

    this.onFormChange("tableName");
  }

  /* TODO:: for now, only source operators support attribute autocomplete.
  * Later, to enable autocomplete for the rest operators,
  * remove this function as well as *ngIf in side-bar.component.html
  */
  isSourceOperator(): boolean {
    if (this.operatorTitle.toLowerCase().search(".*source*") === 0) {
      return true;
    } else return false;
  }
}
