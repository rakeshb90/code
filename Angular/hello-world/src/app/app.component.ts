import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  appName = 'Angular-Project';
  moduleName='HR-Module';
  HR={
    name:"HR1",
    email:"hr1@gmail.com",
    mobile:"8210786828"

  }
  url=window.location.href;
}
