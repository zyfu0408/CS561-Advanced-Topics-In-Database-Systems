package project3.problem2;

import java.io.FileWriter;
import java.io.IOException;

//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class jsonSet {

	public static void main(String[] args) {

		// TODO Auto-generated method stub

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		customer cus = new customer(); 
		int len = 10;
		createRandom rm = new createRandom();
		
		
		try {
			FileWriter fw = new FileWriter("customer.json");
			
			for (int i = 0; i < len; i++) {
				cus.setCustomerID(Integer.toString(i));
				cus.setName(rm.createRandomString(100));
				cus.setAddress(rm.createRandomString(100));
				cus.setSalary(Integer.toString(rm.createRandomInt(100, 1000)));
				cus.setGender(rm.createRandomBinary());
				String json = gson.toJson(cus);
				
/*				obj.put("CustomerID", Integer.toString(i));
				obj.put("Name", rm.createRandomString(100));
				obj.put("Address", rm.createRandomString(100));
				obj.put("Salary", rm.createRandomInt(100, 1000));
				obj.put("Gender", rm.createRandomBinary());*/
				
				//fw.write(obj.toJSONString());
				//System.out.println(json);
				fw.write(json);
				if(i < len -1){
					fw.write(",");
					fw.write(System.getProperty("line.separator"));}
			}
			fw.close();
			System.out.println("Done!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
