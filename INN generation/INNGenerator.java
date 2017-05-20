package inn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class INNGenerator {

	public static void main(String[] args) {
		final Random rand = new Random();
	    
		String result = "";

		for(int i = 0; i < 10000; i++){
			int region = rand.nextInt(99) + 1 ;
			if (region < 10){
				result += "0";
			}
			result += region;
			
			result += String.valueOf(rand.nextInt(1000-100)+100) + String.valueOf(rand.nextInt(100000-10000)+10000) + " ";
		}
		
		try{
            Files.write(Paths.get("inn.txt"),result.getBytes());
        }catch(IOException e){
            e.printStackTrace();
        }
	}
}
