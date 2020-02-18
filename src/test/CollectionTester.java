package test;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

public class CollectionTester {
	
	/**
	 * Things to consider testing
	 * 
	 * Queries:
	 * 	Find all
	 * 	Find with relational select
	 * 		Conditional operators
	 * 		Embedded documents
	 * 		Arrays
	 * 	Find with relational project
	 * 
	 * Inserts
	 * Updates
	 * Deletes
	 * 
	 * getDocument (done?)
	 * drop
	 */
	
	
	@Test
	public void testFindAll(){
		DB db = new DB("data");
		DBCollection dbc = db.getCollection("test");
		DBCursor results = dbc.find();
		
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
	
	public DBCollection insertTestData(boolean projection){
		DB db = new DB("select");
		DBCollection dbc = db.getCollection("test");
		dbc.drop();
		dbc = db.getCollection("test");

		if(!projection){
			String json = "{ \"key\":\"value\" , \"number\": \"b\"}";
			JsonObject res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value\", \"number\": \"ba\" }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value2\" }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":{\"key2\": \"value\"} , \"number\": \"be\"}";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":[\"value1\", \"value2\"] }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value1\", \"number\": \"f\"}";
			res = Document.parse(json);
			dbc.insert(res);
		}else{
			String json = "{ \"key\":\"value\" , \"number\": \"b\", \"color\": \"black\"}";
			JsonObject res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value\", \"number\": \"ba\", \"color\": \"black\" }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value2\" }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":{\"key2\": \"value\"} , \"number\": \"be\"}";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":[\"value1\", \"value2\"] }";
			res = Document.parse(json);
			dbc.insert(res);
			json = "{ \"key\":\"value1\", \"number\": \"f\", \"color\": \"white\"}";
			res = Document.parse(json);
			dbc.insert(res);
		}
		
		return dbc;
	}
	
	//test Find With Relational Select
	@Test
	public void testSimpleQuery(){
		
		DBCollection dbc = insertTestData(false);
		//test simple query
		String json = "{ \"key\":\"value\" }";
		JsonObject query = Document.parse(json);
		DBCursor results = dbc.find(query);
		assertTrue(results.count() == 2);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); //pull second document
		json = "{\"key\":\"value\",\"number\":\"b\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d2 = results.next();//pull last document
		json = "{\"key\":\"value\",\"number\":\"ba\"}";
		assertTrue(Document.toJsonString(d2).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testQueryTwoKeys(){
		DBCollection dbc = insertTestData(false);
		//test query two keys
		String json = "{ \"key\":\"value\", \"number\": \"b\"}";
		JsonObject query = Document.parse(json);
		DBCursor results = dbc.find(query);
		assertTrue(results.count() == 1);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); //pull second document
		json = "{\"key\":\"value\",\"number\":\"b\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testEmbeddedQuery(){
		DBCollection dbc = insertTestData(false);
		//test embedded
		String json = "{ \"key\":{\"key2\": \"value\"}}";
		JsonObject query = Document.parse(json);
		DBCursor results = dbc.find(query);
		assertTrue(results.count() == 1);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); //pull second document
		json = "{\"key\":{\"key2\":\"value\"},\"number\":\"be\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testArrayQuery(){
		DBCollection dbc = insertTestData(false);
		//test embedded
		String json = "{ \"key\":[\"value1\", \"value2\"] }";
		JsonObject query = Document.parse(json);
		DBCursor results = dbc.find(query);
		assertTrue(results.count() == 1);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); //pull second document
		json = "{\"key\":[\"value1\",\"value2\"]}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testConditionalOperators(){
		DBCollection dbc = insertTestData(false);
		//test Conditional operators
		String json = "{ \"number\":{\"$gte\": \"ba\"}}";
		JsonObject query = Document.parse(json);
		DBCursor results = dbc.find(query);
		assertTrue(results.count() == 3);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); //pull second document
		json = "{\"key\":\"value\",\"number\":\"ba\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d2 = results.next();//pull last document
		json = "{\"key\":{\"key2\":\"value\"},\"number\":\"be\"}";
		assertTrue(Document.toJsonString(d2).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d3 = results.next();//pull last document
		json = "{\"key\":\"value1\",\"number\":\"f\"}";
		assertTrue(Document.toJsonString(d3).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
		
		json = "{ \"number\":{\"$eq\": \"ba\"}}";
		query = Document.parse(json);
		results = dbc.find(query);
		assertTrue(results.count() == 1);
		
		json = "{ \"number\":{\"$gt\": \"ba\"}}";
		query = Document.parse(json);
		results = dbc.find(query);
		assertTrue(results.count() == 2);
		
		json = "{ \"number\":{\"$lt\": \"ba\"}}";
		query = Document.parse(json);
		results = dbc.find(query);
		assertTrue(results.count() == 1);
		
		json = "{ \"number\":{\"$lte\": \"ba\"}}";
		query = Document.parse(json);
		results = dbc.find(query);
		assertTrue(results.count() == 2);
		
		json = "{ \"number\":{\"$ne\": \"ba\"}}";
		query = Document.parse(json);
		results = dbc.find(query);
		assertTrue(results.count() == 3);
	}
	
	@Test
	public void testSimpleProjecttion(){
		
		DBCollection dbc = insertTestData(true);
		
		//test simple query & select number
		String json = "{ \"key\":\"value\" }";
		String json2 = "{\"number\":\"1\"}";
		JsonObject query = Document.parse(json);
		JsonObject projection = Document.parse(json2);
		DBCursor results = dbc.find(query, projection);
		assertTrue(results.count() == 2);
		
		assertTrue(results.hasNext());
		JsonObject d1 = results.next(); 
		json = "{\"number\":\"b\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext()); 
		JsonObject d2 = results.next();
		json = "{\"number\":\"ba\"}";
		assertTrue(Document.toJsonString(d2).equals(json));
		
		assertFalse(results.hasNext());//no more documents
	}
		
	@Test
	public void testProjectOneKey(){
		DBCollection dbc = insertTestData(true);
		
		//test Conditional operators & filter out key
		String json = "{ \"number\":{\"$gte\": \"ba\"}}";
		String json2 = "{\"key\":\"0\"}";
		JsonObject query = Document.parse(json);
		JsonObject projection = Document.parse(json2);
		DBCursor results = dbc.find(query, projection);
		assertTrue(results.count() == 3);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); 
		json = "{\"number\":\"ba\",\"color\":\"black\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d2 = results.next();
		json = "{\"number\":\"be\"}";
		assertTrue(Document.toJsonString(d2).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d3 = results.next();
		json = "{\"number\":\"f\",\"color\":\"white\"}";
		assertTrue(Document.toJsonString(d3).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testProjectMoreKey(){
		DBCollection dbc = insertTestData(true);
		//test Conditional operators & filter out two key
		String json = "{ \"number\":{\"$gte\": \"ba\"}}";
		String json2 = "{\"key\":\"0\", \"number\":\"0\"}";
		JsonObject query = Document.parse(json);
		JsonObject projection = Document.parse(json2);
		DBCursor results = dbc.find(query, projection);
		assertTrue(results.count() == 3);
		
		assertTrue(results.hasNext());//still more documents
		JsonObject d1 = results.next(); 
		json = "{\"color\":\"black\"}";
		assertTrue(Document.toJsonString(d1).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d2 = results.next();
		json = "{}";
		assertTrue(Document.toJsonString(d2).equals(json));//verify contents
		
		assertTrue(results.hasNext()); //still one more document
		JsonObject d3 = results.next();
		json = "{\"color\":\"white\"}";
		assertTrue(Document.toJsonString(d3).equals(json));//verify contents
		
		assertFalse(results.hasNext());//no more documents
	}
	
	@Test
	public void testInsertDocument() {
		DB db = new DB("hw5");
		DBCollection test = db.getCollection("test");
		String json = "{ \"key\":\"value\" }";//setup
		JsonObject results = Document.parse(json);
		test.insert(results);
		assertTrue(test.getDocument(0).getAsJsonPrimitive("key").getAsString().equals("value"));
		test.drop();
	}
	
	@Test
	public void testUpdateAllDocument(){
		DB db = new DB("update");
		DBCollection test = db.getCollection("test");
		String json = "{ \"key\":\"value\" }";
		JsonObject results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value\" }";
		results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value2\" }";
		results = Document.parse(json);
		test.insert(results);
		
		json = "{ \"key\":\"value\" }";
		JsonObject query = Document.parse(json);
		json = "{ \"key\":\"value3\" }";
		JsonObject update = Document.parse(json);
		test.update(query, update, true);
		
		assertTrue(test.getDocument(0).getAsJsonPrimitive("key").getAsString().equals("value3"));
		assertTrue(test.getDocument(1).getAsJsonPrimitive("key").getAsString().equals("value3"));
		assertTrue(test.getDocument(2).getAsJsonPrimitive("key").getAsString().equals("value2"));
		test.drop();
	}
	
	@Test
	public void testUpdateFirstDocument(){
		DB db = new DB("update");
		DBCollection test = db.getCollection("test");
		String json = "{ \"key\":\"value\" }";
		JsonObject results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value\" }";
		results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value2\" }";
		results = Document.parse(json);
		test.insert(results);
		
		json = "{ \"key\":\"value\" }";
		JsonObject query = Document.parse(json);
		json = "{ \"key\":\"value3\" }";;
		JsonObject update = Document.parse(json);
		test.update(query, update, false);
		
		assertTrue(test.getDocument(0).getAsJsonPrimitive("key").getAsString().equals("value3"));
		assertTrue(test.getDocument(1).getAsJsonPrimitive("key").getAsString().equals("value"));
		assertTrue(test.getDocument(2).getAsJsonPrimitive("key").getAsString().equals("value2"));
		test.drop();
	}
	
	@Test
	public void testDeleteAllMatchDocument(){
		DB db = new DB("delete");
		DBCollection test = db.getCollection("testDelete");
		String json = "{ \"key\":\"value\" }";
		JsonObject results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value\" }";
		results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value2\" }";
		results = Document.parse(json);
		test.insert(results);
		
		json = "{ \"key\":\"value\" }";
		JsonObject query = Document.parse(json);
		test.remove(query, true);
		
		assertTrue(test.count() == 1);
		assertTrue(test.getDocument(0).getAsJsonPrimitive("key").getAsString().equals("value2"));
		test.drop();
	}
	
	@Test
	public void testDeleteFirstMatchDocument(){
		DB db = new DB("delete");
		DBCollection test = db.getCollection("testDelete");
		String json = "{ \"key\":\"value\" }";
		JsonObject results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value\" }";
		results = Document.parse(json);
		test.insert(results);
		json = "{ \"key\":\"value2\" }";
		results = Document.parse(json);
		test.insert(results);
		
		json = "{ \"key\":\"value\" }";
		JsonObject query = Document.parse(json);
		test.remove(query, false);
		
		assertTrue(test.count() == 2);
		assertTrue(test.getDocument(0).getAsJsonPrimitive("key").getAsString().equals("value"));
		assertTrue(test.getDocument(1).getAsJsonPrimitive("key").getAsString().equals("value2"));
		test.drop();
	}
	
	@Test
	public void testGetDocument() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		JsonObject primitive = test.getDocument(0);
		assertTrue(primitive.getAsJsonPrimitive("key").getAsString().equals("value"));
	}
	
	@Test
	public void testDrop(){
		DB db = new DB("drop");
		DBCollection test = db.getCollection("testDrop");
		test.drop();
		assertFalse(new File("drop/testDrop").exists());
	}
	
}
