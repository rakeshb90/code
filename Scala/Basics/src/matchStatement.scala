

object matchStatement {
  def main(args:Array[String]){
  val age=18;
  // match used as statement
    age match{
      case 15=>println("age: "+age);
      case 18=>println("age: "+age);
      case 20=>println("age: "+age);
      case 25=>println("age: "+age);
    }
    // match used as expression
    val result=age match{
      case 15=>"age: "+age;
      case 18=>"age: "+age;
      case 20=>"age: "+age;
      case 25=>"age: "+age;
    }
    println("result: "+result);
  }
  
}