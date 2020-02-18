package hw5;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import sun.launcher.resources.launcher;

public class DBCollection {

	/**
	 * Constructs a collection for the given database
	 * with the given name. If that collection doesn't exist
	 * it will be created.
	 */
	File file;
	String name;
	String json;
	public DBCollection(DB database, String name) {
		this.name = name;
		json = "";
		file = new File(database.getPath() + "/" + name + ".json");
		if(!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		readFile();
	}
	
	/**
	 * Returns a cursor for all of the documents in
	 * this collection.
	 */
	public DBCursor find() {
		readFile();
		return new DBCursor(this, null, null);
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		readFile();
		return new DBCursor(this, query, null);
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @param projection relational project
	 * @return
	 */
	public DBCursor find(JsonObject query, JsonObject projection) {
		readFile();
		return new DBCursor(this, query, projection);
	}
	
	/**
	 * Inserts documents into the collection
	 * Must create and set a proper id before insertion
	 * When this method is completed, the documents
	 * should be permanently stored on disk.
	 * @param documents
	 */
	public void insert(JsonObject... documents) {
		readFile();
		StringBuilder sb = new StringBuilder();
		for(JsonObject document:documents){
			if(count() > 0 || sb.length() > 0)
				sb.append("\t\r");
			sb.append(Document.toJsonString(document));
		}
		try {
			FileWriter fw = new FileWriter(file,true);
			fw.write(sb.toString());
			fw.flush();
			fw.close();
			readFile();
//			RandomAccessFile ra = new RandomAccessFile(file, "rw");
//			ra.seek(ra.length());
//			ra.writeBytes(sb.toString());
//			readFile();
//			ra.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Locates one or more documents and replaces them
	 * with the update document.
	 * @param query relational select for documents to be updated
	 * @param update the document to be used for the update
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void update(JsonObject query, JsonObject update, boolean multi) {
		readFile();
		String[] splits = json.split("\t\r");
		for(int i = 0; i < count(); i++){
			boolean shouldUpdate = true;
			JsonObject js = getDocument(i);
			for(Map.Entry<String, JsonElement> e:query.entrySet()){
				if(!js.has(e.getKey()) || !js.getAsJsonPrimitive(e.getKey()).getAsString().equals(e.getValue().getAsJsonPrimitive().getAsString())){
					shouldUpdate = false;
					break;
				}
			}
			if(shouldUpdate){
				for(Map.Entry<String, JsonElement> e:update.entrySet()){
					js.remove(e.getKey());
					js.add(e.getKey(), e.getValue());
				}
				splits[i] = Document.toJsonString(js);
				if(!multi)
					break;
			}
		}
		StringBuilder sb = new StringBuilder();
		for(String split:splits){
			if(sb.length() > 0)
				sb.append("\t\r");
			sb.append(split);
		}
		try {
			FileWriter fw = new FileWriter(file);
			fw.write(sb.toString());
			fw.flush();
			fw.close();
			readFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Removes one or more documents that match the given
	 * query parameters
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		readFile();
		String[] splits = json.split("\t\r");
		Set<Integer> delete = new HashSet<>();
		for(int i = 0; i < count(); i++){
			boolean shouldDelete = true;
			JsonObject js = getDocument(i);
			for(Map.Entry<String, JsonElement> e:query.entrySet()){
				if(!js.has(e.getKey()) || !js.getAsJsonPrimitive(e.getKey()).getAsString().equals(e.getValue().getAsJsonPrimitive().getAsString())){
					shouldDelete = false;
					break;
				}
			}
			if(shouldDelete){
				delete.add(i);
				if(!multi)
					break;
			}
		}
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < splits.length; i++){
			if(delete.contains(i)) continue;
			if(sb.length() > 0)
				sb.append("\t\r");
			sb.append(splits[i]);
		}
		try {
			FileWriter fw = new FileWriter(file);
			fw.write(sb.toString());
			fw.flush();
			fw.close();
			readFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns the number of documents in this collection
	 */
	public long count() {
		if(json.length() == 0) return 0;
		String[] splits = json.split("\t\r");
		return splits.length;
	}
	
	public String getName() {
		return name;
	}
	
	/**
	 * Returns the ith document in the collection.
	 * Documents are separated by a line that contains only a single tab (\t)
	 * Use the parse function from the document class to create the document object
	 */
	public JsonObject getDocument(int i) {
		String[] splits = json.split("\t\r");
		return Document.parse(splits[i]);
	}
	
	public void readFile(){
		try {
			InputStream in = new FileInputStream(file);
			byte[] data = new byte[in.available()];
			in.read(data);
			json = new String(data);
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Drops this collection, removing all of the documents it contains from the DB
	 */
	public void drop() {
		file.delete();
	}
	
}
