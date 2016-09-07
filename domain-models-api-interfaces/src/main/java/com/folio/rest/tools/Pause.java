package com.folio.rest.tools;

public class Pause {

	public static void main(String[] args) {
		try {
			System.out.println("sleeping ----------------------------------------------------------------------- ");

			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
