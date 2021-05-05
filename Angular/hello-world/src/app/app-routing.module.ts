import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HrListComponent } from './hr-list/hr-list.component';
import { NewHrComponent } from './new-hr/new-hr.component';

const routes: Routes = [
  {path:'hr',component:NewHrComponent},
  {path:'list',component:HrListComponent},
  {path:'',redirectTo:'/list',pathMatch:'full'}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
