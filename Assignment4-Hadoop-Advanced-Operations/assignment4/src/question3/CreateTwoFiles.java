package question3;

//create points file and k centroids file
public class CreateTwoFiles {
	Q3Util util;
	int k;
	
	public CreateTwoFiles(int k) {
		//k is the number of centroid points
		this.k = k;
		util = new Q3Util();
	}
	
	public static void main(String[] args) {
		//3 is the default number of the centroid points in our program
		//but feel free to change 7 to any other integer you want
		new CreateTwoFiles(7).lanuch();
	}
	
	private void lanuch() {
		util.createPointsFile();
		util.createKCentroids(k);
	}
}
