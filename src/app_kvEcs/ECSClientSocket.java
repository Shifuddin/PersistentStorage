/**
 * Client socket for ecs to connect with server node*/

package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import org.apache.commons.lang3.SerializationUtils;
import common.messages.KVMessage;
import common.messages.KVMessageImp;
import common.messages.KVMessage.StatusType;

public class ECSClientSocket {
	private int port;
	private String address;
	private OutputStream output;
 	private InputStream input;
 	private Socket clientSocket;
 	
	public ECSClientSocket (int port, String address)
	{
		this.port = port;
		this.address = address;
	}
	
	public void connect() throws UnknownHostException, IOException  {
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		
	}

	public void disconnect() {
		// TODO Auto-generated method stub
		if(clientSocket != null) {
			try {
				this.closeConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			clientSocket = null;
		}
		
	}
	private void closeConnection() throws IOException
	{
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			
		}
	}
	/**
	 * send array of bytes using output stream
	 * @param byte array
	 */
	public void send(byte [] msgBytes) throws IOException
	{
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}
	public byte [] receiveConnectionMsg()
	{
		int c;
		StringBuilder sb = new StringBuilder();
		try {
			while ((c = input.read()) != 13) {
				sb.append((char) c);
			}
			return sb.toString().getBytes();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			return null;
		}

	}
	/**
	 * send array of bytes and transform into KVMessage object using unmarshalling
	 * @return KVMessage object
	 * @throws IOException 
	 */
	public KVMessage receive() throws IOException 
	{
			
		int c;
		byte[] array = new byte[500000];

		c = input.read(array);
	
		byte[] resultbuf = new byte[c];
		System.arraycopy(array, 0, resultbuf, 0, c);

		byte [] arr1 = new byte [resultbuf[0]];
		byte [] arr2 = new byte [resultbuf[1]];
		byte [] arr3 = new byte [resultbuf[2]];
		byte [] arr4 = new byte [resultbuf[3]*127 + resultbuf[4]];
		
		
		
		System.arraycopy(resultbuf, 5, arr1, 0, arr1.length);
		System.arraycopy(resultbuf, 5+arr1.length, arr2, 0, arr2.length);
		System.arraycopy(resultbuf, 5+arr1.length + arr2.length, arr3, 0, arr3.length);
		System.arraycopy(resultbuf, 5+arr1.length + arr2.length + arr3.length, arr4, 0, arr4.length);
		
		
		
		
		StatusType status = null;
		String strStatus = new String (arr3);
		
		if (strStatus.trim().equals("GET"))
		{
			status= StatusType.GET;
		}
		else if (strStatus.trim().equals("GET_ERROR"))
		{
			status= StatusType.GET_ERROR;
		}
		else if (strStatus.trim().equals("GET_SUCCESS"))
		{
			status= StatusType.GET_SUCCESS;
		}
		else if (strStatus.trim().equals("PUT"))
		{
			status= StatusType.PUT;
		}
		else if (strStatus.trim().equals("PUT_SUCCESS"))
		{
			status= StatusType.PUT_SUCCESS;
		}
		else if (strStatus.trim().equals("PUT_UPDATE"))
		{
			status= StatusType.PUT_UPDATE;
		}
		else if (strStatus.trim().equals("PUT_ERROR"))
		{
			status= StatusType.PUT_ERROR;
		}
		else if (strStatus.trim().equals("DELETE_SUCCESS"))
		{
			status= StatusType.DELETE_SUCCESS;
		}
		else if (strStatus.trim().equals("DELETE_ERROR"))
		{
			status= StatusType.DELETE_ERROR;
		}
		else if (strStatus.trim().equals("SERVER_STOPPED"))
		{
			status= StatusType.SERVER_STOPPED;
		}
		else if (strStatus.trim().equals("SERVER_WRITE_LOCKED"))
		{
			status= StatusType.SERVER_WRITE_LOCKED;
		}
		else if (strStatus.trim().equals("SERVER_NOT_RESPONSIBLE"))
		{
			status= StatusType.SERVER_NOT_RESPONSIBLE;
		}
		
		
		KVMessage message = new KVMessageImp(new String(arr1), new String (arr2), status, SerializationUtils.deserialize(arr4));
	    return message;
	    
  
	}
	/**
	 * Receives first connection acceptance string from the server
	 * @return 
	 * 
	 * @return: String
	 */
	public String receiveMsg() throws IOException{
		int c;
		int count = 0;
		StringBuilder sb = new StringBuilder();
		try {
			while ((c = input.read()) != 13) {
				sb.append((char) c);
				count++;
				if (count > 100)
					break;
			}
			if(count > 100)
				return "";
			return sb.toString();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			return null;
		}

	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
