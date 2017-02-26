/**
 * Performs hash function on a given key
 * @author shifuddin, Ilya
 */
package common.messages;

import java.io.UnsupportedEncodingException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class KVHashFunction implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2677618889120572997L;

	/**Return has of a given value
	 * @param value {@link String}
	 * @return {@link BigInteger}
	 * */
	public BigInteger hash(String value)
	{
		byte[] bytesOfMessage;
		BigInteger bigInt = null;
		try {
			bytesOfMessage = value.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] thedigest = md.digest(bytesOfMessage);
			
			bigInt = new BigInteger(1,thedigest);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return bigInt;

	}

}
