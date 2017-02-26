package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	

	@Test
	public void testDeleteDisconnect() {
		String key = "deleteTestValue";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.disconnect();
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}
		
		assertNotNull(ex);
	}
	
	@Test
	public void testDeleteError() {
		String key = "deleteTestValue";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}
	
	
	
	@Test
	public void testGetWrongValue() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && !response.getValue().equals("bar1"));
	}
	
	@Test
	public void testGetKey() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getKey().equals("foo"));
	}
	
	@Test
	public void testGetWrongKey() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && !response.getKey().equals("foo1"));
	}

	
	@Test
	public void testGetDisconnec() {
		
		String key = "foo";
		String value = "bar";
		Exception ex = null;
		KVMessage response = null;
		try {
			kvClient.put(key, value);
			kvClient.disconnect();
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

}
