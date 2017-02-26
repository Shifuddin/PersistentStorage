package app_kvServer;

import java.io.IOException;
import java.util.List;

public interface DBFileInterface {
	/**
	 * Returns all the keys in the file
	 * @return {@link List}
	 * */
	public List<String> getAllKey();
	
	/**Get value of a key
	 * @param key {@link String}
	 * @return {@link String}
	 * */
	public String get(String key);
	
	/**Inserts key and value pair into the file
	 * @param key {@link String}
	 * @param value {@link String}
	 * @return {@link Void}
	 * */
	public void put(String key, String value);
	
	/**
	 * Erase contents of any file
	 * @return {@link Void}
	 * */
	public void eraseContent() throws IOException;

}
