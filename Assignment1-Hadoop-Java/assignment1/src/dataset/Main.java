package dataset;

public class Main {

	public static void main(String[] args) {
		Transaction t = new Transaction();
		Customer c = new Customer();
		
		t.launch();
		c.launch();
		
		System.out.println("Files Created!!!");
	}

}
