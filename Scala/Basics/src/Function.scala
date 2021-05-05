import java.util.Date;

object Function {
  object Math{
    def div(x:Int,y:Int):Int={
    return x/y;
  }
  }
  def add(x:Int,y:Int):Int={
    return x+y;
  }
   def sub(x:Int,y:Int):Int={
     x-y;
  }
    def mul(x:Int,y:Int)=x*y;
    // as like  funtional  interface
    def higherFun(a:Int,b:Int,f:(Int,Int)=>Int):Int=f(a,b);
    // log mesage by using partial applied fun
    def logMessage(date:Date,message:String){
      println(date+" "+message);
      
    }
    
  def main(args:Array[String]){
    println("Add 3,4:" + add(3, 4));
    println("sub 6,4:" + sub(6, 4));
    println("mul 3,4:" + mul(3, 4));
    println("div 9,4:" + Math.div(9, 4));
    val sum = higherFun(10, 20, (a, b) => a + b);
    val multi = higherFun(10, 20, (a, b) => a * b);// fully applied function
    println("Result of Higher function:" + sum);
    println("Result of Higher function:" + multi);
    /////////************************************////
    var date=new Date();
    val message="You have message";
    val newLog=logMessage(date,_:String);//partially applied function
    println("***********************************************")
    newLog(message);
    newLog(message);
    newLog(message);
    newLog(message);

  }
}