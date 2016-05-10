package project3.problem1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class rectangleSet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int len = 8000000;
		rectangle rec = new rectangle();
		StringBuffer str = new StringBuffer();
		createRandom rm = new createRandom();
		
		FileWriter fw;
		try {
			fw = new FileWriter("rectangle.txt");
			BufferedWriter bw = new BufferedWriter(fw,1);
			
			for(int i = 0; i<len; i++){
				
				//set x
				rec.setTopleftX(rm.createRamdomInt(1, 10000));
				str.append(rec.getTopleftX());
				str.append(',');
				
				//set y
				rec.setTopleftY(rm.createRamdomInt(1, 10000));
				str.append(rec.getTopleftY());
				str.append(',');
				
				//set width
				rec.setWidth(rm.createRamdomInt(1, 5));
				str.append(rec.getWidth());
				str.append(',');
				
				//set length
				rec.setLength(rm.createRamdomInt(1, 20));
				str.append(rec.getLength());
				
				bw.write(str.toString());
				bw.newLine();
				str.setLength(0);
			}
			bw.flush();
			bw.close();
			fw.close();
			System.out.println("Done!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
