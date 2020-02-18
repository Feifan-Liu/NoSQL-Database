package test;

import static org.junit.Assert.*;

import org.junit.Test;
import com.google.gson.JsonObject;

import hw5.Document;

public class DocumentTester {
	
	/*
	 * Things to consider testing:
	 * 
	 * Invalid JSON
	 * 
	 * Properly parses embedded documents
	 * Properly parses arrays
	 * Properly parses primitives (done!)
	 * 
	 * Object to embedded document
	 * Object to array
	 * Object to primitive
	 */
	
	@Test
	public void testParsePrimitive() {
		String json = "{ \"key\":\"value\" }";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(results.getAsJsonPrimitive("key").getAsString().equals("value")); //verify results
	}
	
	@Test
	public void testParseEmbeddedDocments(){
		String json = "{ \"key\":{\"key2\": \"value\"} }";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(results.getAsJsonObject("key").getAsJsonPrimitive("key2").getAsString().equals("value")); //verify results
	}
	
	@Test
	public void testParseArray(){
		String json = "{ \"key\":[\"value1\", \"value2\"] }";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(results.getAsJsonArray("key").get(0).getAsString().equals("value1")); //verify results
		assertTrue(results.getAsJsonArray("key").get(1).getAsString().equals("value2")); //verify results
	}
	
	@Test
	public void testObject2Primitive(){
		String json = "{\"key\":\"value\"}";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(Document.toJsonString(results).equals(json));
	}
	
	@Test
	public void testObject2EmbeddedDocument(){
		String json = "{\"key\":{\"key2\":\"value\"}}";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(Document.toJsonString(results).equals(json));
	}
	
	@Test
	public void testObject2Array(){
		String json = "{\"key\":[\"value1\",\"value2\"]}";//setup
		JsonObject results = Document.parse(json); //call method to be tested
		assertTrue(Document.toJsonString(results).equals(json));
	}

}
