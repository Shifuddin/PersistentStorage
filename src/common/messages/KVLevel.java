/**
 * Maps key to specific level and threshold value
 * @author shifuddin, Ilya
 */
package common.messages;

import java.util.HashMap;

public class KVLevel {
	
	HashMap<String, Integer> hashLevel1;
	HashMap<String, Integer> hashLevel2;
	HashMap<String, Integer> hashLevel3;
	
	public KVLevel()
	{
		initialize();
	}
	
	/**Initialize hashmap of each level
	 * Puts pumping station parameters as key and threshold as value
	 * @return Void*/
	private void initialize()
	{
		hashLevel1 = new HashMap<String, Integer>();
		hashLevel2 = new HashMap<String, Integer>();
		hashLevel3 = new HashMap<String, Integer>();
		
		
		hashLevel1.put("fuel", 90);
		hashLevel1.put("water", 70);
		hashLevel2.put("gas", 85);
		hashLevel2.put("air", 75);
		hashLevel3.put("temperature", 70);
	}
	
	/**Returns level of any given key
	 * @param String
	 * @return String
	 * */
	public String getLevel(String key) {
		if (hashLevel1.containsKey(key))
			return "level1";
		else if (hashLevel2.containsKey(key))
			return "level2";
		else if (hashLevel3.containsKey(key))
			return "level3";
		else
			return "nolevel";
	}
	
	/**Returns boolean value whether threshold reached or not for some specific level, key, value
	 * @return Boolean
	 * */
	public boolean isThresholdReached(String level, String key, String value)
	{
		if (level.equals("level1"))
		{
			try
			{
				if (Integer.parseInt(value) < hashLevel1.get(key))
				return true;
				else
					return false;
			}
			catch (Exception e)
			{
				return false;
			}
		}
		else if (level.equals("level2"))
		{
			try
			{
				if (Integer.parseInt(value) < hashLevel2.get(key))
				return true;
				else
					return false;
			}
			catch (Exception e)
			{
				return false;
			}
		}
		else if (level.equals("level3"))
		{
			try
			{
				if (Integer.parseInt(value) < hashLevel3.get(key))
				return true;
				else
					return false;
			}
			catch (Exception e)
			{
				return false;
			}
		}
		else
			return false;
		
	}
	
}
