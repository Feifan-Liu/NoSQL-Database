package test;


import java.io.File;

import static org.junit.Assert.*;

import org.junit.Test;

import hw5.DB;
import hw5.DBCollection;

public class DBTester {
	
	/**
	 * Things to consider testing:
	 * 
	 * Properly creates directory for new DB (done)
	 * Properly accesses existing directory for existing DB
	 * Properly accesses collection
	 * Properly drops a database
	 * Special character handling?
	 */
	
	@Test
	public void testCreateDB() {
		DB hw5 = new DB("hw5"); //call method
		assertTrue(new File("testfiles/hw5").exists()); //verify results
	}
	
	@Test
	public void tsetAccessExistingDB(){
		assertTrue(new File("testfiles/data/test.json").exists()); //verify test DB exists
		DB data = new DB("data");
		assertTrue(data.getPath().equals("testfiles/data"));
		assertTrue(new File("testfiles/data/test.json").exists()); //do not change file by accessing
	}
	
	@Test
	public void testAccessCollection(){
		DB data = new DB("data");
		DBCollection dbc = data.getCollection("test");
		assertTrue(dbc.getName().equals("test"));
	}
	
	@Test
	public void testDropDB(){
		DB drop = new DB("drop"); 
		assertTrue(new File("testfiles/drop").exists()); //verify results
		drop.dropDatabase();
		assertFalse(new File("testfiles/drop").exists());
	}
	
	@Test
	public void testSpecialChar(){
		DB hw5 = new DB("d@#!03_t"); //call method
		assertTrue(new File("testfiles/d@#!03_t").exists()); //verify results
	}

}
