/**
* @author Md Shiffudin Al Masud,Vidrashku Ilya
* @version 1.1
*/
package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

import javax.print.DocFlavor.BYTE_ARRAY;

import org.apache.commons.lang3.SerializationUtils;

import common.messages.KVConsistentHash;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImp;

public class KVStore implements KVCommInterface {

	private OutputStream output;
	private InputStream input;
	private Socket clientSocket;
	private String address;
	private int port;
	private String conSucString;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {

		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws IllegalArgumentException, IOException {
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		receiveMsg();

	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (clientSocket != null) {
			try {
				this.closeConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			clientSocket = null;
		}

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		byte [] metaBytes = SerializationUtils.serialize(null);
		byte [] total = totalByteArray(key.getBytes(), value.getBytes(), StatusType.PUT.toString().getBytes(), metaBytes);
		sendDataAsync(total).call();
		KVMessage msg = receiveDataAsync().call();
		return msg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub

		byte [] metaBytes = SerializationUtils.serialize(null);
		byte [] total = totalByteArray(key.getBytes(), "x".getBytes(), StatusType.GET.toString().getBytes(), metaBytes);
		sendDataAsync(total).call();
		KVMessage msg = receiveDataAsync().call();
		return msg;
	}

	private void closeConnection() throws IOException {
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;

		}
	}

	/**
	 * send array of bytes using output stream in an asynchronized way
	 *@param msgBytes {@link BYTE_ARRAY}
	 *@return {@link Callable}
	 */
	
	public Callable<Boolean> sendDataAsync(byte[] msgBytes) {
		Callable<Boolean> callable = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				return true;
			}
		};
		return callable;

	}
	
	/**
	 * Receives byte array from input stream
	 * Divides into parts by reading the header bits
	 * Finally performs unmarshalling on each part
	 * @return {@link Callable}
	 * */
	public Callable<KVMessage> receiveDataAsync() {
		Callable<KVMessage> callable = new Callable<KVMessage>() {
			@Override
			public KVMessage call() throws Exception {

				int c;
				byte[] array = new byte[500000];

				c = input.read(array);

				byte[] receiveTotal = new byte[c];
				System.arraycopy(array, 0, receiveTotal, 0, c);
				KVMessage message = null;
				if (receiveTotal.length > 0) {
					int keyLengthSlot = receiveTotal[0];
					int totalKeyByte = 0;

					if (keyLengthSlot > 0) {
						for (int i = 1; i <= keyLengthSlot; i++) {
							totalKeyByte = totalKeyByte + receiveTotal[i];
						}
						byte[] keyByte = new byte[totalKeyByte];
						System.arraycopy(receiveTotal, keyLengthSlot + 1, keyByte, 0, keyByte.length);

						int valueLengthSlot = receiveTotal[keyLengthSlot + 1 + keyByte.length];
						int totalValueByte = 0;
						if (valueLengthSlot > 0) {

							for (int i = 1; i <= valueLengthSlot; i++) {
								totalValueByte = totalValueByte + receiveTotal[keyLengthSlot + 1 + keyByte.length + i];
							}
							byte[] valueByte = new byte[totalValueByte];
							System.arraycopy(receiveTotal, keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1,
									valueByte, 0, valueByte.length);

							int statusSlotLength = receiveTotal[keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1
									+ valueByte.length];
							byte[] statusByte = new byte[statusSlotLength];
							System.arraycopy(receiveTotal,
									keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1 + valueByte.length + 1,
									statusByte, 0, statusByte.length);

							byte[] metadataByte = new byte[receiveTotal.length - (keyLengthSlot + 1 + keyByte.length
									+ valueLengthSlot + 1 + valueByte.length + 1 + statusSlotLength)];
							
							System.arraycopy(
									receiveTotal, keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1
											+ valueByte.length + 1 + statusSlotLength,
									metadataByte, 0, metadataByte.length);
							KVConsistentHash<String> receivedMeta = SerializationUtils.deserialize(metadataByte);
							message = new KVMessageImp(new String(keyByte), new String(valueByte),
									getStatus(statusByte), receivedMeta);
						} else {
							System.out.println("No value");
						}
					} else {
						System.out.println("No key");
					}

				} else {
					System.out.println("No data");
				}

				return message;

			}
		};
		return callable;
	}
	/**Given a byte array unmarshal into string
	 * Returns status matches with string
	 * @return {@link StatusType}
	 * @param array {@link BYTE_ARRAY}
	 * */
	private StatusType getStatus(byte[] array) {
		StatusType status = null;
		String strStatus = new String(array);

		if (strStatus.trim().equals("GET")) {
			status = StatusType.GET;
		} else if (strStatus.trim().equals("GET_ERROR")) {
			status = StatusType.GET_ERROR;
		} else if (strStatus.trim().equals("GET_SUCCESS")) {
			status = StatusType.GET_SUCCESS;
		} else if (strStatus.trim().equals("PUT")) {
			status = StatusType.PUT;
		} else if (strStatus.trim().equals("PUT_SUCCESS")) {
			status = StatusType.PUT_SUCCESS;
		} else if (strStatus.trim().equals("PUT_UPDATE")) {
			status = StatusType.PUT_UPDATE;
		} else if (strStatus.trim().equals("PUT_ERROR")) {
			status = StatusType.PUT_ERROR;
		} else if (strStatus.trim().equals("DELETE_SUCCESS")) {
			status = StatusType.DELETE_SUCCESS;
		} else if (strStatus.trim().equals("DELETE_ERROR")) {
			status = StatusType.DELETE_ERROR;
		} else if (strStatus.trim().equals("SERVER_STOPPED")) {
			status = StatusType.SERVER_STOPPED;
		} else if (strStatus.trim().equals("SERVER_WRITE_LOCKED")) {
			status = StatusType.SERVER_WRITE_LOCKED;
		} else if (strStatus.trim().equals("SERVER_NOT_RESPONSIBLE")) {
			status = StatusType.SERVER_NOT_RESPONSIBLE;
		} else if (strStatus.trim().equals("SUB_S")) {
			status = StatusType.SUB_S;
		} else if (strStatus.trim().equals("UNSUB_S")) {
			status = StatusType.UNSUB_S;
		}
		else if (strStatus.trim().equals("SERVER_REPLICA_SUC1")) {
			status = StatusType.SERVER_REPLICA_SUC1;
		}
		else if (strStatus.trim().equals("SERVER_REPLICA_SUC2")) {
			status = StatusType.SERVER_REPLICA_SUC2;
		}
		return status;

	}


	/**
	 * Receives first connection acceptance string from the server
	 * 
	 * @return: String
	 */
	public void receiveMsg() throws IOException {
		int c;
		StringBuilder sb = new StringBuilder();
		try {
			while ((c = input.read()) != 13) {
				sb.append((char) c);
			}
			conSucString = sb.toString();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			conSucString = null;
		}

	}
	
	/**Sends string as byte array
	 * */
	public void sendMessage(String conMsg) throws IOException {
		byte[] msgBytes = conMsg.getBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}


	public String getConSucString() {
		return conSucString;
	}
	
	/**
	 * Merge four byte array into one byte array
	 * Puts some header field so that it can be recognized in the receiver 
	 * Length of key or value or statustype or metadata are given as header field
	 * Since we can't present more than 127 as byte if the length of any byte array 
	 * is more than 127 this method divides the length into different parts 
	 * Finally place another byte field to point out how many fields are header fields for length
	 * @return BYTE_ARRAY
	 * @param BYTE_ARRAY, BYTE_ARRAY, BYTE_ARRAY, BYTE_ARRAY*/
	private  byte[] totalByteArray(byte[] keyBytes, byte[] valueBytes, byte[] statusTypeByte, byte[] metaBytes) {
		int keyByteTotal = keyBytes.length;
		int keyByteSlotCount = 0;
		int valueByteTotal = valueBytes.length;
		int valueByteSlotCount = 0;

		byte statusTypeLength = (byte) statusTypeByte.length;

		while (keyByteTotal > 127) {
			keyByteSlotCount++;
			keyByteTotal = keyByteTotal - 127;
		}
		if (keyByteTotal > 0) {
			keyByteSlotCount++;
		}

		while (valueByteTotal > 127) {
			valueByteSlotCount++;
			valueByteTotal = valueByteTotal - 127;
		}
		if (valueByteTotal > 0) {
			valueByteSlotCount++;
		}

		byte[] totalByte = new byte[keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1 + valueBytes.length
				+ 1 + statusTypeLength + metaBytes.length];
		// Copy slot count
		for (int i = 0; i <= keyByteSlotCount; i++) {
			if (i == 0)
				totalByte[i] = (byte) keyByteSlotCount;
			else if (i == keyByteSlotCount)
				totalByte[i] = (byte) keyByteTotal;
			else
				totalByte[i] = 127;
		}
		// Copy content
		System.arraycopy(keyBytes, 0, totalByte, keyByteSlotCount + 1, keyBytes.length);

		// Copy slot count
		for (int i = 0; i <= valueByteSlotCount; i++) {
			if (i == 0)
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = (byte) valueByteSlotCount;
			else if (i == valueByteSlotCount)
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = (byte) valueByteTotal;
			else
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = 127;
		}
		// Copy content
		System.arraycopy(valueBytes, 0, totalByte, keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1,
				valueBytes.length);
		totalByte[keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1
				+ valueBytes.length] = statusTypeLength;
		System.arraycopy(statusTypeByte, 0, totalByte,
				keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1 + valueBytes.length + 1,
				statusTypeByte.length);
		System.arraycopy(metaBytes, 0, totalByte, keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1
				+ valueBytes.length + 1 + statusTypeByte.length, metaBytes.length);
		return totalByte;

	}

}
