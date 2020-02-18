package test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.gson.JsonObject;
//import com.sun.xml.internal.txw2.Document;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

public class CursorTester {
	
	/**
	 * Things to consider testing:
	 * 
	 * hasNext (done?)
	 * count (done?)
	 * next (done?)
	 */

	@Test
	public void testFindAll() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find();
		
		assertTrue(results.count() == 3);
		assertTrue(results.hasNext());
		String json = "{\"key\":\"value\"}";
		JsonObject d1 = results.next(); //pull first document
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d2 = results.next(); //pull second document
		json = "{\"embedded\":{\"key2\":\"value2\"}}";
		assertTrue(Document.toJsonString(d2).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d3 = results.next();//pull last document
		json = "{\"array\":[\"one\",\"two\",\"three\"]}";
		assertTrue(Document.toJsonString(d3).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
}
