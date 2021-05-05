import { Component, OnInit,Output } from '@angular/core';
import * as EventEmitter from 'node:events';

@Component({
  selector: 'app-test',
  templateUrl: './test.component.html',
  styleUrls: ['./test.component.css']
})
export class TestComponent implements OnInit {
  @Output() parentFun:EventEmitter = new EventEmitter();
  constructor() { }

  ngOnInit(): void {
  }
  sendToParent(){
    let data={name:"Ram",age:21};


  }
}
