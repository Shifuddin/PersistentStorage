/**
 * Very simple connection class
 * Sends and receives messages from server nodes
 * */

package app_KvControlRoom;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

public class ServerConnection extends Thread {
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private boolean isOpen;
	private static Logger logger = Logger.getRootLogger();

	public ServerConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		isOpen = true;
	}

	/**
	 * Receives connection message, performs marshalling and sends as byte array to 
	 * newly connected server
	 * @param String
	 * @return Void
	 * @throws IOException
	 * */
	private void sendMessage(String conMsg) throws IOException {
		byte[] msgBytes = conMsg.getBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}

	/**Receives byte array from server node, performs unmarshalling
	 * and return as String 
	 * @return String
	 * @throws IOException
	 * */
	public String receiveMsg() throws IOException {
		int c;
		byte[] array = new byte[500000];

		c = input.read(array);
		byte[] receiveTotal = new byte[c];
		System.arraycopy(array, 0, receiveTotal, 0, c);
		return new String(receiveTotal);

	}

	/**Overloaded methods from parent class Thread
	 * Sends connection message and receives updates from server nodes
	 * @return Void
	 */
	public void run() {

		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			sendMessage("Connection to MSRG Echo server established: " + clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort() + '\r');

			while (isOpen) {
				try {
					logger.info(receiveMsg());
					System.out.print("EchoControlRoom> ");
				} catch (NullPointerException | NegativeArraySizeException | IOException ioe) {
					isOpen = false;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
