/**
 * Given a hash function, list of nodes and the size of the list 
 * This class responsible to bring back a virtual ring of nodes
 * Where each node has some space in the ring
 * Each and every datum of this entire universe must fit into any of the node space in the ring
 * @author shifuddin, Ilya*/

package common.messages;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.print.DocFlavor.STRING;

import java.util.Map.Entry;
import java.io.Serializable;

public class KVConsistentHash<T> implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3382333548084299434L;
	private final KVHashFunction hashFunction;
	private final SortedMap<BigInteger, String> circle = new TreeMap<BigInteger, String>();

	public KVConsistentHash(KVHashFunction hashFunction, List<String> nodes, int numberOfNodes) {
	 this.hashFunction = hashFunction;
	 for (int i = 0; i < numberOfNodes; i++)
		 add(nodes.get(i));

 }
	/**Add new node in the ring
	 * @param node {@link String} 
	 * @return Void*/
	public void add(String node) {

		circle.put(hashFunction.hash(node), node);
	}

	/**Remove a node from the ring
	 * @param node STRING*/
	public void remove(String node) {	
		circle.remove(hashFunction.hash(node));
	}
	
	
	/**Returns the node in which data space given key will fall
	 * @return {@link STRING}
	 * @param key {@link String}
	 * */
	public String get(String key) {
		if (circle.isEmpty()) {
			return null;
		}
		BigInteger hash = hashFunction.hash(key);
		if (!circle.containsKey(hash)) {
			SortedMap<BigInteger, String> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
	
	/**Prints virtual ring
	 * @return Void
	 * */
	public void showPositions()
	{
		for(Map.Entry<BigInteger,String> entry : circle.entrySet()) {
			  BigInteger key = entry.getKey();
			  String value = entry.getValue();

			  System.out.println(key + " => " + value);
			}
		
	}
	/**Returns first succeeding node of a given node
	 * @return {@link String}
	 * @param node {@link String}
	 * */
	public String getPredecessor(String node)
	{
		Iterator entry = circle.entrySet().iterator();
		
		while (entry.hasNext())
		{
			Entry thisEntry = (Entry) entry.next();
			String value = (String) thisEntry.getValue();
			if (value.equals(node))
			{
				if (entry.hasNext())
				{
					thisEntry = (Entry) entry.next();
					value = (String) thisEntry.getValue();	
					return value;
				}
				else
				{
					return first();
				}
			}
	
		}
		return "not found";

	}
	
	/**Returns second succeeding node of a given node
	 * @return {@link String}
	 * @param node {@link String}
	 * */
	public String getSePredecessor(String node)
	{
		Iterator entry = circle.entrySet().iterator();
		Entry thisEntry;
		String value;
		while (entry.hasNext())
		{
			thisEntry = (Entry) entry.next();
			value = (String) thisEntry.getValue();
			if (value.equals(node))
			{
				if (entry.hasNext())
				{
					thisEntry = (Entry) entry.next();
					
					if (entry.hasNext())
					{
						thisEntry = (Entry) entry.next();
						value = (String) thisEntry.getValue();	
						return value;
					}
					else
						return first();
				}
				else
				{
					return second();
				}
			}
	
		}
		return "not found";

	}
	/**Returns first preceding node of a given node
	 * @return {@link String}
	 * @param node {@link String}
	 * */
	public String getSuccessor(String node)
	{
		Iterator entry = circle.entrySet().iterator();
		
		String previous = null;
		while (entry.hasNext())
		{
			Entry thisEntry = (Entry) entry.next();
			String value = (String) thisEntry.getValue();
			if (value.equals(node))
			{
				if (previous == null)
				{
					return last();
				}
				else
					return previous;
			}
			previous = value;
		}
		return "Not found";
		
	}
	
	/**Returns second preceding node of a given node
	 * @return {@link String}
	 * @param node {@link String}
	 * */
	public String getSeSuccessor(String node)
	{
		Iterator entry = circle.entrySet().iterator();
		String array [] = new String[circle.size()];
		int i = 0;
		String value;
		while (entry.hasNext())
		{
			Entry thisEntry = (Entry) entry.next();
			value = (String) thisEntry.getValue();
			array[i] = value;
			i++;
		}
		
		for ( i = 0; i < array.length; i++)
		{
			if (array[i].equals(node))
			{
				if ( i - 2 < 0)
				{
					if (circle.size() == 1)
						return array[0];
					return array[i - 2 + circle.size()];
				}
				else
				{
					return array[i-2];
				}
			}
		}
		return "Not found";
		
	}
	/**Returns last node in the ring
	 * @return {@link String}
	 * */
	private String last()
	{
		String value = null;
		for(Map.Entry<BigInteger,String> entry : circle.entrySet()) {
			  value = entry.getValue();
			}
		return value;
	}
	/**Returns all the nodes of the ring
	 * @return {@link List}
	 * */
	public List<String> getAll()
	{
		List <String> list = new ArrayList<String>();
		String value = null;
		for(Map.Entry<BigInteger,String> entry : circle.entrySet()) {
			  value = entry.getValue();
			  list.add(value);
			}
		return list;
	}
	/**Returns first node in the ring
	 * @return {@link String}
	 * */
	private String first()
	{
		String value = null;
		for(Map.Entry<BigInteger,String> entry : circle.entrySet()) {
			  value = entry.getValue();
			  break;
			}
		return value;
	}
	
	/**Returns second node in the ring
	 * @return {@link String}
	 * */
	private String second()
	{
		String value = null;
		int i = 0;
		for(Map.Entry<BigInteger,String> entry : circle.entrySet()) {
			  value = entry.getValue();
			  
			  if ( i == 1)
				  break;
			  i++;
			}
		return value;
	}
	
	public int getSize()
	{
		return circle.size();
	}
	
}