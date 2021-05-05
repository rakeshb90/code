import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  inputVal:string='';
  title = 'angular-basics';
  name:string="Krishna";
  color:string="blue";
  errColor=true;
  HR=[
    {
      name:"Raj",
      age:28
    },
    {
      name:"Saj",
      age:38
    },
    {
      name:"Baj",
      age:48
    },
    {
      name:"Ram",
      age:58
    }


  ]  
  getName(){
    alert("You have enter "+this.inputVal);
  }
  buttonUse(){
    console.log();
  }
  getVal(val:string){
    console.warn(val);
    this.inputVal=val;
  }
  disableBox=true;
  enableBox(){
    this.disableBox=false;
  }
  show=false;
  showTag(){
    this.show=true;
  }
  formVal(fval:any){
    console.log(fval);
    alert(fval.name);

  }
changeColor(){
  this.errColor=!this.errColor;
}
detail="Test Detail for test component"
  parentFun(){
    

  }
}
