package project3.problem1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import project3.problem1.point;


public class pointSet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int len = 10;
		point pnt = new point();
		StringBuffer str = new StringBuffer();
		createRandom rm = new createRandom();
		
		FileWriter fw;
		try {
			fw = new FileWriter("point.txt");
			BufferedWriter bw = new BufferedWriter(fw,1);
			
			for(int i = 0; i<len; i++){
				
				//set x
				pnt.setX(rm.createRamdomInt(1, 10000));
				str.append(pnt.getX());
				str.append(',');
				
				//set y
				pnt.setY(rm.createRamdomInt(1, 10000));
				str.append(pnt.getY());
				
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
