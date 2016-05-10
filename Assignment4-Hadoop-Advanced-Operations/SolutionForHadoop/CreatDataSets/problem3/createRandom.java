package project3.problem3;

import java.util.Random;

public class createRandom {
	public int createRamdomInt(int min, int max){

        Random random = new Random();
        int s = random.nextInt(max - min + 1) + min;
        
        return s;
	}

}
