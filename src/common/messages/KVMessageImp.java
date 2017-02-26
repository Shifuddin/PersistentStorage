/** Implementation of KVMessage Interface
* @author Md Shiffudin Al Masud,Vidrashku Ilya
* @version 1.1
*/

package common.messages;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;

public class KVMessageImp implements KVMessage, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7394437769490302306L;
	private String key;
	private String value;
	private StatusType status;
	private KVConsistentHash<String> metadata;
	
	public KVMessageImp(String key, String value, StatusType status, KVConsistentHash<String> metadata ) {
		// TODO Auto-generated constructor stub
		this.key = key;
		this.value = value;
		this.status = status;
		this.metadata = metadata;
		
				
	}
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return status;
	}
	@Override
	public byte[] getBytes()
	{
		byte[] arr1 = key.getBytes(); 
		byte[] arr2 = value.getBytes();
		byte[] arr3 = status.toString().getBytes();
		byte[] complete = new byte[arr1.length + arr2.length + arr3.length];
		System.arraycopy(arr1, 0, complete, 0, arr1.length);
		System.arraycopy(arr2, 0, complete, arr1.length, arr2.length);
		System.arraycopy(arr3, 0, complete, arr1.length+arr2.length, arr3.length);
		return complete;
		
		
//		byte[] arr4 = SerializationUtils.serialize( metadata);
//		byte[] arr5 = bigIntTobyte(arr4.length);
		
		
//		byte[] arr1 = key.getBytes(); 
//		byte[] arr2 = value.getBytes();
//		byte[] arr3 = status.toString().getBytes();
//		byte[] arr4 = SerializationUtils.serialize( metadata);
//		byte[] arr5 = bigIntTobyte(arr4.length);
//		
//		byte[] complete = new byte[arr1.length + arr2.length + arr3.length + arr4.length+ 5];
//		complete[0] = (byte)arr1.length;
//		complete[1] = (byte)arr2.length;
//		complete[2] = (byte)arr3.length;
//		complete[3] = (byte) (arr5.length-1); 
//		complete[4]= arr5[arr5.length-1];
//		
//		
//		System.arraycopy(arr1, 0, complete, 5, arr1.length);
//		System.arraycopy(arr2, 0, complete, arr1.length+5, arr2.length);
//		System.arraycopy(arr3, 0, complete, arr1.length+5+arr2.length, arr3.length);
//		System.arraycopy(arr4, 0, complete, arr1.length+5+arr2.length+ arr3.length, arr4.length);
//		
//		
//		return complete;

	}
	@Override
	public KVConsistentHash<String> getMetadata()
	{
		return metadata;
	}
	private byte[] bigIntTobyte(int a)
	{
		int b = a/127;
		
		byte [] arr = new byte[b+1];
		
		int i = 0;
		while( a > 127)
		{
			
			arr[i] = 127;
			a = a - 127;
			i++;
		}
		arr[i] = (byte)a;
		return arr;

	}
	

}
