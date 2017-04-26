import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule }   from '@angular/forms';
import { HttpModule }    from '@angular/http';

import { AppComponent }  from './app.component';
import { TheFlowchartComponent }   from './flowchart/the-flowchart.component';

import { BsDropdownModule } from 'ng2-bootstrap/';
import { Ng2Bs3ModalModule } from 'ng2-bs3-modal/ng2-bs3-modal';

@NgModule({
  imports:      [ BsDropdownModule.forRoot(),
      BrowserModule,
      FormsModule,
      HttpModule,
      Ng2Bs3ModalModule,
	],
  declarations: [ AppComponent,
    TheFlowchartComponent
	],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }
