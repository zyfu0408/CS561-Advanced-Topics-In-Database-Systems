package dataset;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import util.Util;

public class Transaction {
	Util util;
	
	public Transaction() {
		util = new Util();
	}
	
	private int getRandomCustID() {
		return util.getRandomNumber(1, 50000);
	}
	
	private float getRandomTransTotal() {
		return util.getRandomFloatNumber(10, 1000);
	}
	
	private int getRandomTransNumItems() {
		return util.getRandomNumber(1, 10);
	}
	
	private String getRandomTransDesc() {
		return util.getRandomCharacters(20, 50);
	}
	
	public void launch() {
		PrintWriter pw = null;
		
		try {
			pw = new PrintWriter(new FileWriter("file/transactions_2.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int i = 1; i <= 5000; i++) {
			pw.println(i + "," + getRandomCustID() + "," + getRandomTransTotal() + ","
					+ getRandomTransNumItems() + "," + getRandomTransDesc());
		}
		
		pw.close();
		System.out.println("done");
	}
	
}
