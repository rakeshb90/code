

object ReduceFoldScan {
  def main(args:Array[String]){
    val lst=List(1,2,3,4);
    val charLst=List("A","B","C");
    println(lst.reduceLeft(_+_));
    println("***************************")
    println(lst.reduceLeft(_-_));
    println("***************************")
    println(lst.reduceLeft((x,y)=>{println(x+" "+y); x-y}));
    println("***************************")
    println(lst.reduceRight((x,y)=>{println(x+" "+y); x-y}));
    println("***************************")
    println(lst.foldLeft(100)(_+_));
    println("***************************")
    println(lst.scanLeft(10)(_+_));
     println(charLst.scanLeft("Z")(_+_));
     println(charLst.scanRight("Z")(_+_));
    
    
  }
  
}