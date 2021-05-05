

object forLoop {
  def main(args:Array[String]){
    for(i <- 1 to 5){
      print(i);
    }
    println();
     for(i <- 1 .to(5)){
      print(i);
    }
     println();
      for(i <- 1 until 5){
      print(i);
    }
      println("Nested loop");
       for(i <- 1 until 8;j<- 1 to 3){
      println("i="+i+"j="+j);
    }
        var lst=List(1,2,4,45,12,6);
        println("list data");
         for(i<- lst){
           print(i+" ");
         }
         println("\n filter list data");
         for(i<- lst;if i<7){
           print(i+" ");
         }
         val result= for {i<- lst;if i<7} yield {
            i*i
         }
         println("square Result: "+result);
  }
  
 
  
}