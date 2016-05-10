package dataset;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import util.Util;

public class Customer {
	Util util;
	
	public Customer() {
		util = new Util();
	}
	
	private String getRandomName() {
		return util.getRandomCharacters(10, 20);
	}
	
	private int getRandomAge() {
		return util.getRandomNumber(10, 70);
	}
	
	private int getRandomCode() {
		return util.getRandomNumber(1, 10);
	}
	
	private float getRandomSalary() {
		return util.getRandomFloatNumber(100, 10000);
	}
	
	public void launch() {
		PrintWriter pw = null;
		
		try {
			pw = new PrintWriter(new FileWriter("file/customers.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int i = 1; i <= 50000; i++) {
			pw.println(i + "," + getRandomName() + "," + getRandomAge() + ","
					+ getRandomCode() + "," + getRandomSalary());
		}
		
		pw.close();
		System.out.println("done");
	}

}
