package com.rakesh.spark.wordCount;

/**
 * Hello world!
 *
 */
public class App 
{
	
    public static void main( String[] args )
    {
        System.out.println( "***********Word count spark project********" );
        if(args[0].length()==0) {
        	System.out.println("input File not found.... ");
        	System.exit(0);
        }
       WordCounter counter=new WordCounter();
       counter.wordCount("input.txt");
       
    }
}
