package hw5;

import java.io.File;
import java.io.IOException;

public class DB {

	/**
	 * Creates a database object with the given name.
	 * The name of the database will be used to locate
	 * where the collections for that database are stored.
	 * For example if my database is called "library",
	 * I would expect all collections for that database to
	 * be in a directory called "library".
	 * 
	 * If the given database does not exist, it should be
	 * created.
	 */
	private String path;
	public DB(String name) {
		path = "testfiles/" + name;
		File file = new File(path);
		if(!file.exists()){
			file.mkdir();
		}
	}
	
	public String getPath(){
		return path;
	}
	
	/**
	 * Retrieves the collection with the given name
	 * from this database. The collection should be in
	 * a single file in the directory for this database.
	 * 
	 * Note that it is not necessary to read any data from
	 * disk at this time. Those methods are in DBCollection.
	 */
	public DBCollection getCollection(String name) {
		return new DBCollection(this, name);
	}
	
	/**
	 * Drops this database and all collections that it contains
	 */
	public void dropDatabase() {
		File file = new File(path);
		file.delete();
	}
	
	
}
