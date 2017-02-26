package app_kvServer;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class LRUCache extends LinkedHashMap<String, String> implements CacheInterface {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 3348718829576115166L;
	private int capacity;
    
    public LRUCache(int capacity, boolean accOrder) {
        super(capacity+1, 1.0f, accOrder);  // for access order
        this.capacity = capacity;
    }
    
    public String get(String key) {
    	
        if(super.get(key) == null)
            return null;
        else
            return super.get(key);
    }
    
    public String put(String key, String value) {
        super.put(key, value);
        return null;
    }
    @Override
    protected boolean removeEldestEntry(Entry entry) {
        return (size() > this.capacity);
    }
}