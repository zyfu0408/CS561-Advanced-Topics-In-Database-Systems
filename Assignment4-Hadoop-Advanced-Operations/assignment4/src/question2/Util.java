package question2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Util {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Util util = new Util();
//		sb.append("[\n");
		try {
			FileWriter fw = new FileWriter("customer_info.txt");

			for (int i = 0; i < 400000; i++) {
				StringBuffer sb = new StringBuffer();
				sb.append("{CustomerID:" + i + ",\n");
				sb.append(" Name:" + util.getCharacter() + ",\n");
				sb.append(" Address:" + util.getCharacter() + ",\n");
				sb.append(" Salary:" + util.getSalary() + ",\n");
				sb.append(" Gender:" + util.getGender() + ",\n");
				sb.append("},\n");
				if(i+1 == 400000){
					sb.substring(0, sb.length()-2);
				}
				fw.write(sb.toString());
			}
			
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private Integer getCustomerID(Integer index) {
		return index;
	}
	
	private Integer getSalary() {
		int min = 100;
		int max = 1000;
		return min + (int)(Math.random() * (max - min + 1));
	}
	
	private String getGender() {
		Random ran = new Random();
		boolean value = ran.nextBoolean();
		if (value) {
			return "male";
		} else {
			return "female";
		}
	}
	
	private String getCharacter() {
		Random ran = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < 100; i++) {
			char c = (char)(ran.nextInt(26) + 'a');
			sb.append(c);
		}
		return sb.toString();
	}
	
}
