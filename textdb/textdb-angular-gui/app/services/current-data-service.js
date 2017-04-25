"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
var core_1 = require("@angular/core");
var Subject_1 = require("rxjs/Subject");
var http_1 = require("@angular/http");
var http_2 = require("@angular/http");
require("rxjs/add/operator/catch");
require("rxjs/add/operator/map");
require("rxjs/add/operator/toPromise");
var clean_data = {
    top: 20,
    left: 20,
    properties: {
        title: 'Operator',
        inputs: {},
        outputs: {}
    }
};
var CurrentDataService = (function () {
    function CurrentDataService(http) {
        this.http = http;
        this.newAddition = new Subject_1.Subject();
        this.newAddition$ = this.newAddition.asObservable();
        this.checkPressed = new Subject_1.Subject();
        this.checkPressed$ = this.checkPressed.asObservable();
        this.textdbUrl = 'http://localhost:8080/newqueryplan/execute';
    }
    CurrentDataService.prototype.getData = function () {
        return this.currentData;
    };
    CurrentDataService.prototype.setData = function (data) {
        this.currentData = { id: 1, jsonData: data };
    };
    CurrentDataService.prototype.addData = function (operatorData, operatorNum, allData) {
        this.newAddition.next({ operatorNum: operatorNum, operatorData: operatorData });
        this.setData(allData);
    };
    CurrentDataService.prototype.selectData = function (operatorNum) {
        var data_now = jQuery("#the-flowchart").flowchart("getOperatorData", operatorNum);
        this.newAddition.next({ operatorNum: operatorNum, operatorData: data_now });
        this.setData(jQuery("#the-flowchart").flowchart("getData"));
    };
    CurrentDataService.prototype.clearData = function () {
        this.newAddition.next({ operatorNum: 0, operatorData: clean_data });
    };
    CurrentDataService.prototype.processData = function () {
        this.TEXTDBJSON = { operators: {}, links: {} };
        var operators = [];
        var links = [];
        var listAttributes = ["attributes", "dictionaryEntries"];
        for (var operatorIndex in this.currentData.jsonData.operators) {
            var currentOperator = this.currentData.jsonData['operators'];
            if (currentOperator.hasOwnProperty(operatorIndex)) {
                var attributes = {};
                attributes["operatorID"] = operatorIndex;
                for (var attribute in currentOperator[operatorIndex]['properties']['attributes']) {
                    if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)) {
                        attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
                        // if attribute is an array property, and it's not an array
                        if (jQuery.inArray(attribute, listAttributes) != -1 && !Array.isArray(attributes[attribute])) {
                            attributes[attribute] = attributes[attribute].split(",").map(function (item) { return item.trim(); });
                        }
                        // if the value is a string and can be converted to a boolean value
                        if (attributes[attribute] instanceof String && Boolean(attributes[attribute])) {
                            attributes[attribute] = (attributes[attribute].toLowerCase() === 'true');
                        }
                    }
                }
                operators.push(attributes);
            }
        }
        for (var link in this.currentData.jsonData.links) {
            var destination = {};
            var currentLink = this.currentData.jsonData['links'];
            if (currentLink[link].hasOwnProperty("fromOperator")) {
                destination["origin"] = currentLink[link]['fromOperator'].toString();
                destination["destination"] = currentLink[link]['toOperator'].toString();
                links.push(destination);
            }
        }
        this.TEXTDBJSON.operators = operators;
        this.TEXTDBJSON.links = links;
        console.log("about to send request");
        this.sendRequest();
    };
    CurrentDataService.prototype.sendRequest = function () {
        var _this = this;
        var headers = new http_2.Headers({ 'Content-Type': 'application/json' });
        console.log("TextDB JSON is:");
        console.log(JSON.stringify(this.TEXTDBJSON));
        this.http.post(this.textdbUrl, JSON.stringify(this.TEXTDBJSON), { headers: headers })
            .subscribe(function (data) {
            console.log("OKAY in getting server data");
            _this.checkPressed.next(data.json());
        }, function (err) {
            console.log("Error in getting server data");
            console.log(err);
            _this.checkPressed.next(err.json());
        });
    };
    CurrentDataService.prototype.handleError = function (error) {
        console.error('An error occurred', error); // for demo purposes only
        return Promise.reject(error.message || error);
    };
    CurrentDataService.prototype.getDataSlowly = function () {
        var _this = this;
        return new Promise(function (resolve) {
            // Simulate server latency with 2 second delay
            setTimeout(function () { return resolve(_this.getData()); }, 2000);
        });
    };
    return CurrentDataService;
}());
CurrentDataService = __decorate([
    core_1.Injectable(),
    __metadata("design:paramtypes", [http_1.Http])
], CurrentDataService);
exports.CurrentDataService = CurrentDataService;
//# sourceMappingURL=current-data-service.js.map