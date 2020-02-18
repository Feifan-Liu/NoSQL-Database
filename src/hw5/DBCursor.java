package hw5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DBCursor implements Iterator<JsonObject>{

	int cursor;
	List<JsonObject> jsons;
	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		cursor = 0;
		jsons = new ArrayList<>();
		for(int i = 0; i < collection.count(); i++){
			boolean shouldAdd = true;
			JsonObject js = collection.getDocument(i);
			if(query != null){
				for(Map.Entry<String, JsonElement> e:query.entrySet()){
					JsonElement element = e.getValue();
					if(!js.has(e.getKey())){
						shouldAdd = false;
						break;
					}
//					System.out.println(js.getAsJsonPrimitive(e.getKey()).toString());
					if(element.isJsonObject()){
						JsonElement embed = js.get(e.getKey());
						JsonObject embededQuery = element.getAsJsonObject();
						for(Map.Entry<String, JsonElement> en:embededQuery.entrySet()){
							
							if(!embed.isJsonObject()){
								switch (en.getKey()) {
								case "$eq":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) == 0;
									break;
								case "$gt":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) > 0;
									break;
								case "$gte":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) >= 0;
									break;
								case "$lt":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) < 0;
									break;
								case "$lte":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) <= 0;
									break;
								case "$ne":
									shouldAdd = embed.getAsJsonPrimitive().getAsString().compareTo(en.getValue().getAsJsonPrimitive().getAsString()) != 0;
									break;
								default:
									shouldAdd = false;
									break;
								}
							}else{
								JsonObject embedObj = embed.getAsJsonObject();
								if(!embedObj.has(en.getKey()) || 
										!embedObj.getAsJsonPrimitive(en.getKey()).getAsString().equals(en.getValue().getAsJsonPrimitive().getAsString())){
									shouldAdd = false;
									break;
								}
							}
							if(!shouldAdd)
								break;
						}
						if(!shouldAdd)
							break;
					}else if(element.isJsonArray()){
						if(!js.get(e.getKey()).isJsonArray()){
							shouldAdd = false;
							break;
						}
						JsonArray jsArray = js.getAsJsonArray(e.getKey());
						JsonArray queryArray = element.getAsJsonArray();
						for(JsonElement queryElement:queryArray){
							if(!jsArray.contains(queryElement)){
								shouldAdd = false;
								break;
							}
						}
						if(!shouldAdd)
							break;
					}
					else if(!js.get(e.getKey()).isJsonPrimitive() || !js.getAsJsonPrimitive(e.getKey()).getAsString().equals(e.getValue().getAsJsonPrimitive().getAsString())){
//						System.out.println(js.getAsJsonPrimitive(e.getKey()).getAsString() + " " + e.getValue().getAsJsonPrimitive().getAsString() + " " + js.isJsonPrimitive());
						shouldAdd = false;
						break;
					}
				}
			}
			if(shouldAdd){
				if(fields == null)
					jsons.add(js);
				else{
					JsonObject jsonObject = new JsonObject();
					for(Map.Entry<String, JsonElement> e:fields.entrySet()){
						if(js.has(e.getKey()) && e.getValue().getAsString().equals("1")){
							jsonObject.add(e.getKey(), js.get(e.getKey()));
						}else{
							if(jsonObject.size() == 0)
								jsonObject = js.deepCopy();
							jsonObject.remove(e.getKey());
						}
					}
					jsons.add(jsonObject);
				}
			}
		}
	}
	
	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		return cursor < jsons.size();
	}

	/**
	 * Returns the next document
	 */
	public JsonObject next() {
		return jsons.get(cursor++);
	}
	
	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return jsons.size();
	}

}
