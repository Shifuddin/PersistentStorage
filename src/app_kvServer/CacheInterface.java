/**
 * @author shifuddin, Ilya
 */
package app_kvServer;

public interface CacheInterface {
	/**
	 * Returns value of a key from the cache
	 * @param key {@link Object}
	 * @return {@link String}
	 * */
	public String get(Object key);
	
	/**
	 * Puts key value pair into cache
	 * @param key {@link String}
	 * @param value {@link String}
	 * @return String
	 * */
	public String put(String key, String value);
	
	/**Return size of the cache
	 * @return {@link Integer}
	 * */
	public int size();
	
	/**Removes a key from the cache
	 * @param key {@link Object}
	 * @return {@link String}
	 * */
	public String remove(Object key);
}
