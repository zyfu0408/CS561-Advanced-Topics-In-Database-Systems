package util;

import java.util.Random;

public class Util {
	char[] name = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
			'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', 
			'2', '3', '4', '5', '6', '7', '8', '9', '0', '@', '#', '*', '_', '$', '&'};
	
	public String getRandomCharacters(int min, int max) {
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i <= getRandomNumber(min, max); i++) {
			int nameScope = getRandomNumber(name.length);
			sb.append(name[nameScope]);
		}
		return sb.toString();
	}
	
	int getRandomNumber(int length) {
		int random = (int) (Math.random() * length);
		
		return random;
	}
	
	public int getRandomNumber(int min, int max) {
		int random = min + (int) (Math.random() * (max + 1 - min));
		
		return random;
	}
	
	float getRandomFloatNumber(int length) {
		Random rand = new Random();
		
		return rand.nextFloat() * length; 
	}
	
	public float getRandomFloatNumber(int min, int max) {
		Random rand = new Random();
		
		return min + rand.nextFloat() * (max - min);
	}
}
