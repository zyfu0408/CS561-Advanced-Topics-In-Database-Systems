package question3;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Q3Util {
	Random random = new Random();
	
	private int generateNum() {
		return random.nextInt(10001);
	}
	
	private String generatePoint() {
		return generateNum() + "," + generateNum() + "\n";
	}
	
	public void createPointsFile() {
		try {
			FileWriter fw = new FileWriter("q3_points.txt");
			for (int i = 0; i < 11000000; i++) {
				fw.write(generatePoint());
			}
			fw.flush();
			fw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void createKCentroids(int k) {
		try {
			FileWriter fw = new FileWriter("q3_kcentroids.txt");
			for (int i = 1; i <= k; i++) {
				fw.write("centroid" + i + "," + generatePoint());
			}
			fw.flush();
			fw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}
