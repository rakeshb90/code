import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';

@Component({
  selector: 'app-new-hr',
  templateUrl: './new-hr.component.html',
  styleUrls: ['./new-hr.component.css']
})
export class NewHrComponent implements OnInit {
    
  hrForm:FormGroup;
  fullNameLength=0;
    
  constructor(private fb:FormBuilder) { 
    // this.hrForm=new FormGroup({
    //   fullName:new FormControl(),
    //   email:new FormControl(),
    //   skill:new FormGroup({
    //     skillName:new FormControl(),
    //     experienceInYears:new FormControl(),
    //     proficiency:new FormControl()
    //   })
    // });
    this.hrForm=this.fb.group(
      {
        fullName:[''],
        email:[''],
        skill:this.fb.group({
          skillName:[''],
          experienceInYears:[''],
          proficiency:['beginner']
        })
      }
    );
  }

  ngOnInit(): void {
    // this.hrForm=new FormGroup({
    //   fullName:new FormControl(),
    //   email:new FormControl(),
    //   skill:new FormGroup({
    //     skillName:new FormControl(),
    //     experienceInYears:new FormControl(),
    //     proficiency:new FormControl()
    //   })
    // });
    // by using FormBuilder class:
    this.hrForm=this.fb.group(
      {
        fullName:['',Validators.required],
        email:[''],
        skill:this.fb.group({
          skillName:[''],
          experienceInYears:[''],
          proficiency:['beginner']
        })
      }
    );
  this.hrForm.get('fullName')?.valueChanges.subscribe((value:String)=>{
    this.fullNameLength=value.length;
  })
    }
  onSubmit():void{
    console.log( this.hrForm.controls);
    console.log( this.hrForm.controls.fullName.touched);
    console.log( this.hrForm.get('fullName')?.value);
  }
  onLoad():void{
    this.hrForm.setValue(
      {
        fullName:"Rakesh Roushan",
        email:"rk@gmail.com",
        skill:{
          skillName:"Java",
          experienceInYears:2,
          proficiency:"beginner"
        }

      }
    )
    // this.keyAndValue(this.hrForm);

  }
  keyAndValue(fg:FormGroup):void{
    Object.keys(fg).forEach((key:string)=>{
      const fgControl=fg.get(key);
      if(fgControl instanceof FormGroup)
      this.keyAndValue(fgControl);
      else
      console.log("key: "+key+" Value: "+fgControl?.value);
    })
    

  }
}
