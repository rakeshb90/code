
object MapAndSet {
  def main(args:Array[String]){
    // Map 
    var users:Map[Int,String]=Map(401->"Rk123",404->"rk404",808->"Rk808",808->"Rk808");
    var student:Map[Int,String]=Map(801->"Ram",802->"Raj",803->"Ramesh",808->"Bhola");
    println(users);
     println(users.keys);
      println(users.values);
      users.keys.foreach{
        key=>
           println("Key: "+key+ " Value: "+users(key));
      }
      println("**************************************")
       (student).keys.foreach{
        key=>
           println("Key: "+key+ " Value: "+student(key));
      }
      println(users++student);
      // set 
      val mySet:Set[Int]=Set(1,2,2,3,3,30,3,9,8,7);
      val mySet1:Set[Int]=Set(10,20,2,30,3,3,3,9,80,7);
      println(mySet);
      println(mySet&mySet1);
      mySet.foreach(println);
//      val opt:Option[Int]=None;
      val opt:Option[Int]=Some(9);
      println("Option val: "+opt.get);
  }
}