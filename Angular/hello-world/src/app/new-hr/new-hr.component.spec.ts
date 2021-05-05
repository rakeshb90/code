import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NewHrComponent } from './new-hr.component';

describe('NewHrComponent', () => {
  let component: NewHrComponent;
  let fixture: ComponentFixture<NewHrComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ NewHrComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NewHrComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
