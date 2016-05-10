package project3.problem2;

import java.util.Random;

public class createRandom {
	public int createRandomInt(int min, int max){

        Random random = new Random();
        int s = random.nextInt(max - min + 1) + min;
        
        return s;
	}
	public String createRandomString(int len){
		
		String syllabus = "abcdefghijklmnopqrstuvwxyz";
		StringBuffer strName = new StringBuffer();
		int inx = 0;
		for(int i = 0; i < len; i++){
			inx = createRandomInt(0, syllabus.length()-1);
			strName.append(syllabus.charAt(inx));
		}
		return strName.toString();
	}
	public String createRandomBinary(){
		createRandom rm = new createRandom();
		int s = rm.createRandomInt(0, 1);
		String str = "";
		if (s <= 0.5)
			str = "female";
		else
			str = "male";
		return str;
	}

}
