package _15MultipleValueMapReduce;

import java.util.ArrayList;
import java.util.Scanner;

public class ForEachDemo {
	public static void main(String[] args) {

		Scanner scan = new Scanner(System.in);
		ArrayList<String> list = new ArrayList<String>();

		for (int i = 1; i <= 4; i++) {
			System.out.println("Enter Name " + i);
			list.add(scan.nextLine());
		}

		for (String name : list) {
			System.out.println(name);
		}
	}
}
