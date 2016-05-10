package question1;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Util {
	static Random random = new Random();
	
	private static void createPoints() {
		try {
			FileWriter fw = new FileWriter("points.txt");
			for (int i = 0; i < 8000000; i++) {
				fw.write(i + "," + random.nextInt(10001) + "," + random.nextInt(10001) + "\n");
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void createRectangles(){
		try {
			FileWriter fw = new FileWriter("rectangles.txt");
			for (int i = 0; i < 5000000; i++) {
				fw.write("r" + (i+1) + "," +  random.nextInt(10001) + "," + random.nextInt(10001) + "," + (random.nextInt(20) + 1) + "," + (random.nextInt(5) + 1) + "\n");
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		createPoints();
		createRectangles();
	}
}
