package com.rupa.practice;
import java.util.*;

public class CreArr { //created a class i.e, CreArr for array with public as access modifier
	public static void simarr() { // created a method simarr() which  has public access modifier,static and return type is void
		Scanner sc = new Scanner(System.in); //Scanner is a class in java to read input data from the user
		int [] int_arr = new int [10]; // integer array is declared and intialized with size of 10
		for(int i=0;i < int_arr.length ; i++) { // for loop is created  to store elements until the condition is specified
			int_arr[i] = sc.nextInt(); // read new or next integer  and stored in their respective index.
			System.out.println(int_arr[i]); // print array of each element.
		}
	}

	public static void main(String[] args) {
		simarr(); //calling static method i.e, simarr() (which is created in the class) in main method
	}
	  }
