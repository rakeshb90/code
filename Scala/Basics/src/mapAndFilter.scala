

object mapAndFilter {
  def main(args:Array[String]){
    val list=List(1,2,3,4,5);
    val map=Map(1->"Ram",101->"Raj");
    println(list.map(_*10));
    println("Hello".map(_.toUpper));
    println(list.map(x=>x*10));
    println(map.mapValues(x=>"Hi "+x));
    println(List(List(1,2,3),List(2,9)).flatten);
    println(list.flatMap(x=>List(x,x*100)));
    
  }
}