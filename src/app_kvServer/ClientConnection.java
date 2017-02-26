/**
F * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * The class also implements the echo functionality. Thus whenever a message is
 * received it is going to be echoed back to the client.
 * 
 * @author Md Shiffudin Al Masud,Vidrashku Ilya, SHYAMSUNDAR DEBSARKAR
 * @version 1.1
 * 
 */

package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.print.DocFlavor.BYTE_ARRAY;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.*;

import client.KVStore;
import common.messages.KVConsistentHash;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImp;
import common.messages.KVLevel;

public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private CacheInterface cache;
	private static boolean serverState = true;
	private static boolean lock_write = false;
	private String currentNode;
	private KVConsistentHash<String> metadata;
	private KVServer server;
	private DBFileInterface store, replica_1, replica_2, controlRoom, notificationBuffer;
	private String predecessor;
	private String successor;
	private KVLevel kvLevel;
	private List<String> clientMac;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ClientConnection(KVConsistentHash<String> metadata, Socket clientSocket, CacheInterface cache,
			KVServer server, DBFileInterface store, DBFileInterface replica_1, DBFileInterface replica_2,
			DBFileInterface controlRoom, KVLevel notification, DBFileInterface notificationBuffer) {
		this.clientSocket = clientSocket;
		isOpen = true;
		this.cache = cache;
		this.metadata = metadata;
		this.server = server;
		this.store = store;
		this.replica_1 = replica_1;
		this.replica_2 = replica_2;
		this.controlRoom = controlRoom;
		this.kvLevel = notification;
		this.notificationBuffer = notificationBuffer;
	}

	/**
	 * Each server gossips with it's successor & predecessor sends information
	 * to it's predecessor that i am alive as your successor sends information
	 * to it's successor that i am alive as your predecessor
	 * 
	 * @param String
	 *            predecessor & String successor
	 * @return void
	 * @throws Exception
	 */
	private void gossip(String predecessor, String successor) throws Exception {
		try {
			if (!predecessor.equals(currentNode))
				sendCommand("alive?", predecessor);
		} catch (Exception e) {
			logger.debug("Cant send to " + predecessor);
			sendMessage(predecessor + '\r');
		}
		try {
			if (!successor.equals(currentNode))
				sendCommand("alive?", successor);
		} catch (Exception er) {
			logger.debug("Cant send to " + successor);
			sendMessage(successor + '\r');
		}

	}

	private String getPredecessor() {
		return metadata.getSuccessor(currentNode);

	}

	private String getSuccessor() {
		return metadata.getPredecessor(currentNode);
	}

	/**
	 * Moves a set of key and value to another sever as a client
	 * 
	 * @param String
	 *            key, String value, KVStore client, StatusType status
	 *
	 * @throws Exception
	 */

	private void moveData(String key, String value, KVStore client, StatusType status) throws Exception {
		String[] keys = key.split(":");
		String[] values = value.split(":");

		byte[] metaBytes = SerializationUtils.serialize(null);
		for (int i = 1; i < keys.length; i++) {
			byte[] totalByte = totalByteArray(keys[i].getBytes(), values[i].getBytes(), status.toString().getBytes(),
					metaBytes);
			client.sendDataAsync(totalByte).call();
			Thread.sleep(10);
		}

	}

	/**
	 * Merge four byte array into one byte array Puts some header field so that
	 * it can be recognized in the receiver Length of key or value or statustype
	 * or metadata are given as header field Since we can't present more than
	 * 127 as byte if the length of any byte array is more than 127 this method
	 * divides the length into different parts Finally place another byte field
	 * to point out how many fields are header fields for length
	 * 
	 * @return BYTE_ARRAY
	 * @param BYTE_ARRAY,
	 *            BYTE_ARRAY, BYTE_ARRAY, BYTE_ARRAY
	 */
	private byte[] totalByteArray(byte[] keyBytes, byte[] valueBytes, byte[] statusTypeByte, byte[] metaBytes) {
		int keyByteTotal = keyBytes.length;
		int keyByteSlotCount = 0;
		int valueByteTotal = valueBytes.length;
		int valueByteSlotCount = 0;

		byte statusTypeLength = (byte) statusTypeByte.length;
		// System.out.println("Checking status "+statusTypeLength);
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
		// System.out.println(new String(statusTypeByte));
		System.arraycopy(metaBytes, 0, totalByte, keyByteSlotCount + 1 + keyBytes.length + valueByteSlotCount + 1
				+ valueBytes.length + 1 + statusTypeByte.length, metaBytes.length);
		return totalByte;

	}

	/**
	 * Copy contents of current server to replica nodes
	 * 
	 * @param String
	 *            currentNode, String replica1_Address, String replica2_Address
	 * @throws Exception
	 */
	private void moveDataToSuccessors(String currentNode, String successor1_Address, String successor2_Address)
			throws Exception {
		List<String> l = store.getAllKey();

		if (l.size() > 0) {

			String tKey = "";
			String tValue = "";
			for (int i = 0; i < l.size(); i++) {
				tKey = tKey + ":" + l.get(i);
				tValue = tValue + ":" + store.get(l.get(i));
			}

			if (successor1_Address.equals(currentNode)) {
				String[] keys = tKey.split(":");
				String[] values = tValue.split(":");

				for (int i = 1; i < keys.length; i++) {
					replica_1.put(keys[i], values[i]);
				}
				System.out.println("Storing data to own as replica 1");
			} else {

				// System.out.println("Moving data to " + successor1_Address+ "
				// key " + tKey + " value "+ tValue);
				KVStore client = new KVStore(successor1_Address.trim().split(":")[0],
						Integer.parseInt(successor1_Address.trim().split(":")[1]));
				client.connect();
				// System.out.println("Successfully connected");
				moveData(tKey, tValue, client, StatusType.SERVER_REPLICA_SUC1);
				// System.out.println("Successfully moved");
				client.disconnect();

			}

			if (successor2_Address.equals(currentNode)) {
				String[] keys = tKey.split(":");
				String[] values = tValue.split(":");

				for (int i = 1; i < keys.length; i++) {
					replica_2.put(keys[i], values[i]);
				}
				System.out.println("Storing data to own as replica 2");
			} else {
				// System.out.println("Moving data to " + successor2_Address + "
				// key " + tKey + " value "+ tValue);
				KVStore client = new KVStore(successor2_Address.trim().split(":")[0],
						Integer.parseInt(successor2_Address.trim().split(":")[1]));
				client.connect();

				moveData(tKey, tValue, client, StatusType.SERVER_REPLICA_SUC2);
				client.disconnect();
			}
		}

	}

	/**
	 * At each put operation pair is first transferred to coordinator node
	 * Coordinator node writes pair to it's storage and sends pair to replicas
	 * 
	 * @param String
	 *            key, String value, String replica1_Address, String
	 *            replica2_Address
	 */
	private void writePairToReplicas(String key, String value, String replica1_Address, String replica2_Address)
			throws Exception {

		if (replica1_Address.equals(currentNode)) {

			replica_1.put(key, value);
		} else {

			KVStore client = new KVStore(replica1_Address.trim().split(":")[0],
					Integer.parseInt(replica1_Address.trim().split(":")[1]));
			client.connect();

			moveData(":" + key, ":" + value, client, StatusType.SERVER_REPLICA_SUC1);
			client.disconnect();
		}

		if (replica2_Address.equals(currentNode)) {

			replica_2.put(key, value);
		} else {
			KVStore client = new KVStore(replica2_Address.trim().split(":")[0],
					Integer.parseInt(replica2_Address.trim().split(":")[1]));
			client.connect();

			moveData(":" + key, ":" + value, client, StatusType.SERVER_REPLICA_SUC2);
			client.disconnect();
		}
	}

	/**
	 * Sends command to any replica server
	 * 
	 * @param command,
	 *            replica_address
	 * @return Void
	 * @throws Exception
	 */
	private void sendCommand(String command, String replica_address) throws Exception {
		KVStore client;
		client = new KVStore(replica_address.trim().split(":")[0],
				Integer.parseInt(replica_address.trim().split(":")[1]));
		client.connect();

		byte[] metaBytes = SerializationUtils.serialize(null);
		byte[] totalByte = totalByteArray("command".getBytes(), command.getBytes(),
				StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);
		client.sendDataAsync(totalByte).call();
		client.disconnect();
	}

	private String addressOfLevel(String level) {
		String controlRoomAddress = null;
		List<String> controlRoomInfos = controlRoom.getAllKey();

		for (String controlRoomInfo : controlRoomInfos) {
			if (level.equals(controlRoom.get(controlRoomInfo))) {
				controlRoomAddress = controlRoomInfo;
			}
		}
		return controlRoomAddress;

	}

	/**
	 * Returns list of control rooms which are subscribed to lower level then
	 * given level
	 * 
	 * @param level
	 *            String
	 * @return {@link List}
	 */
	private List<String> getAddressofLowerLevel(String level) {
		List<String> addressLowerLevel = new ArrayList<>();
		if (level.equals("level1")) {
			String addrs;
			if ((addrs = addressOfLevel("level2")) != null) {
				addressLowerLevel.add(addrs);
			}
			if ((addrs = addressOfLevel("level3")) != null) {
				addressLowerLevel.add(addrs);
			}
		} else if (level.equals("level2")) {
			String addrs;
			if ((addrs = addressOfLevel("level3")) != null) {
				addressLowerLevel.add(addrs);
			}
		}
		return addressLowerLevel;
	}

	/**
	 * Example of method overriding Sends key, value and message to particular
	 * control room
	 * 
	 * @param key
	 *            String
	 * @param value
	 *            String
	 * @param address
	 *            String
	 * @param message
	 *            String
	 * @return {@link Boolean}
	 */
	private boolean sendUpdate(String key, String value, String address, String message) {
		try {
			if (message == null)
				sendUpdate("Data has been changed\t<key: " + key + " value: " + value, address);
			else
				sendUpdate(message + " control room is unavailble. " + message + " Data has been changed\t<key: " + key
						+ " value: " + value, address);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	/**
	 * This method is invoked when an update on pair done First of all, it tries
	 * to get the level of that key and then checks whether threshold reaches If
	 * threshold reaches tries to send notification to control room subscribed
	 * to that level If interested control room not found it then sends
	 * notification to control room with lower level If none found, buffer the
	 * notification and sends the buffer to all server nodes
	 * 
	 * @param key
	 *            String
	 * @param value
	 *            String
	 * @return Void
	 */
	private void sendNotification(String key, String value) throws Exception {
		String level = kvLevel.getLevel(key);

		if (kvLevel.isThresholdReached(level, key, value)) {

			String controlRoomAddress = addressOfLevel(level);
			List<String> addressLowerLevel = getAddressofLowerLevel(level);

			if (controlRoomAddress == null) {
				boolean update = false;
				for (String address : addressLowerLevel) {
					update = sendUpdate(key, value, address, level);
				}
				if (update == false) {
					notificationBuffer.put(controlRoomAddress, key + ":" + value);

					for (String node : metadata.getAll()) {
						if (!node.equals(currentNode)) {
							try {
								sendCommand("buffer;" + controlRoomAddress + ";" + key + ":" + value, node);
							} catch (Exception ex) {

							}
						}
					}
				}
			} else {
				boolean update = false;
				update = sendUpdate(key, value, controlRoomAddress, null);
				if (update == false) {
					for (String address : addressLowerLevel) {
						update = sendUpdate(key, value, address, level);
					}
					if (update == false) {
						notificationBuffer.put(controlRoomAddress, key + ":" + value);

						for (String node : metadata.getAll()) {
							if (!node.equals(currentNode)) {
								try {
									sendCommand("buffer;" + controlRoomAddress + ";" + key + ":" + value, node);
								} catch (Exception ex) {

								}
							}
						}
					}

				}

			}
		}
	}

	/**
	 * Sends update message to specific control room First connects with control
	 * room and then sends message asynchronously
	 * 
	 * @param message
	 *            String
	 * @param controlRoomAddress
	 *            String
	 */
	private void sendUpdate(String message, String controlRoomAddress) throws Exception {
		KVStore client;
		client = new KVStore(controlRoomAddress.trim().split(":")[0],
				Integer.parseInt(controlRoomAddress.trim().split(":")[1]));
		client.connect();

		client.sendDataAsync(message.getBytes()).call();
		client.disconnect();
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client. Receives data from client store it if
	 * its put request otherwise retrieves. Send back success or error in both
	 * cases.
	 *
	 */
	public void run() {
		try {
			Runnable sendAlive = new Runnable() {
				public void run() {
					try {
						gossip(getPredecessor(), getSuccessor());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};

			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			// key file
			currentNode = clientSocket.getLocalAddress().toString().substring(1) + ":" + clientSocket.getLocalPort();
			metadata = server.getMetaData();
			cache = server.getCache();

			sendMessage("Connection to MSRG Echo server established: " + clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort() + '\r');

			while (isOpen) {

				try {

					/* Receives data sent by any client */
					KVMessage msg = receiveDataAsyn().call();

					String key = msg.getKey();
					String value = msg.getValue();
					StatusType status = msg.getStatus();

					if (status == StatusType.SERVER_STOPPED) {
						if (key.trim().equals("command")) {

							/* Start servers command starts here */
							/*********************************/
							if (value.trim().equals("start")) {
								serverState = true;
								metadata.showPositions();
								replica_1.eraseContent();
								replica_2.eraseContent();
								moveDataToSuccessors(currentNode, metadata.getPredecessor(currentNode),
										metadata.getSePredecessor(currentNode));

								predecessor = metadata.getSuccessor(currentNode);
								successor = metadata.getPredecessor(currentNode);
								ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
								exec.scheduleAtFixedRate(sendAlive, 0, 40, TimeUnit.SECONDS);

							}
							/* Start servers command ends here */
							/*********************************/

							/* Stop servers command starts here */
							/*********************************/
							else if (value.trim().equals("stop")) {

								serverState = false;
								logger.debug("Received \t<type:" + key + "  name:" + value + " Server Status:" + status
										+ ">");

							}
							/* Stop servers command ends here */
							/*********************************/
							/* Storing as control room starts here */
							else if (value.trim().split(";")[0].equals("subscribe")) {
								String controlRoomAddress = value.trim().split(";")[1];
								String level = value.trim().split(";")[2];
								controlRoom.put(controlRoomAddress, level);
							}
							/* Storing as control room ends here */

							/* Removing control room starts here */
							else if (value.trim().split(";")[0].equals("unsubscribe")) {
								String controlRoomAddress = value.trim().split(";")[1];
								controlRoom.put(controlRoomAddress, "null");
							}
							/* Removing control room ends here */

							/* Buffer starts here */
							else if (value.trim().split(";")[0].equals("buffer")) {
								String controlRoomAddress = value.trim().split(";")[1];
								String keyValuePair = value.trim().split(";")[2];
								notificationBuffer.put(controlRoomAddress, keyValuePair);
							}
							/* Buffer ends here */

							/* Remove buffer starts here */
							else if (value.trim().split(";")[0].equals("removebuffer")) {
								notificationBuffer.put(value.trim().split(";")[1], "null");
							}
							/* Remove buffer ends here */

							/* Remove node commands starts here */
							/*********************************/
							// This command will be executed at first successor
							// of removed node
							else if (value.trim().split(";")[0].equals("transfer")) {
								List<String> list = replica_1.getAllKey();
								if (list.size() > 0) {

									for (String ckey : list) {
										store.put(ckey, replica_1.get(ckey));
									}
								}

								String firstSuccessor = metadata.getPredecessor(currentNode);
								String secondSuccessor = metadata.getSePredecessor(currentNode);
								String firstPredecessor = metadata.getSuccessor(currentNode);
								if (firstSuccessor.equals(currentNode)) {
									replica_1.eraseContent();
									replica_2.eraseContent();
								} else {
									sendCommand("erase_both", firstSuccessor);
								}

								if (secondSuccessor.equals(currentNode)) {
									replica_2.eraseContent();
								} else {
									sendCommand("erase_second", secondSuccessor);
								}

								moveDataToSuccessors(currentNode, firstSuccessor, secondSuccessor);
								replica_1.eraseContent();
								replica_2.eraseContent();
								sendCommand("re_pre_1_r", firstPredecessor);
							}
							// This command will be executed at second successor
							// of removed node
							else if (value.trim().equals("erase_both")) {
								replica_1.eraseContent();
								replica_2.eraseContent();

							}

							// This command will be executed at third successor
							// of removed node
							else if (value.trim().equals("erase_second")) {
								replica_2.eraseContent();

							}
							// This command will be executed at removed node
							else if (value.trim().equals("re_pre_1_r")) {
								String firstSuccessor = metadata.getPredecessor(currentNode);
								String secondSuccessor = metadata.getSePredecessor(currentNode);
								String firstPredecessor = metadata.getSuccessor(currentNode);
								moveDataToSuccessors(currentNode, firstSuccessor, secondSuccessor);
								replica_1.eraseContent();
								sendCommand("re_pre_2_r", firstPredecessor);
							}

							// This command will be executed at first
							// predecessor of removed node
							else if (value.trim().equals("re_pre_2_r")) {
								String firstSuccessor = metadata.getPredecessor(currentNode);
								String secondSuccessor = metadata.getSePredecessor(currentNode);
								moveDataToSuccessors(currentNode, firstSuccessor, secondSuccessor);
							}
							// This command will be executed at removed node
							else if (value.trim().equals("shutdown")) {
								logger.debug("Received \t<type:" + key + "  name:" + value + " Server Status:" + status
										+ ">");
								shutdown();
							}
							/* Remove node commands starts here */
							/*********************************/

							/* Add new node commands starts here */
							/***********************************/

							// This command will be executed at new node
							else if (value.trim().equals("add_new_node")) {
								ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
								exec.scheduleAtFixedRate(sendAlive, 0, 40, TimeUnit.SECONDS);
								serverState = true;
								metadata.showPositions();
								String successor = metadata.getPredecessor(currentNode);
								sendCommand("retrieve_from_successor", successor);
							}
							// This command will be executed at successor of new
							// node
							else if (value.trim().equals("retrieve_from_successor")) {
								lock_write = true;

								String predecessor = metadata.getSuccessor(currentNode);
								System.out.println("p " + predecessor);
								List<String> keyList = store.getAllKey();

								String tKey = "";
								String tValue = "";
								for (String cKey : keyList) {
									if (metadata.get(cKey).equals(predecessor)) {
										tKey = tKey + ":" + cKey;
										tValue = tValue + ":" + store.get(cKey);
									}
								}
								for (String cKey : keyList) {
									if (metadata.get(cKey).equals(predecessor)) {
										store.put(cKey, "null");
										cache.remove(cKey);
									}
								}

								if (tKey.length() > 1) {
									KVStore client = new KVStore(predecessor.trim().split(":")[0],
											Integer.parseInt(predecessor.trim().split(":")[1]));
									client.connect();

									moveData(tKey, tValue, client, StatusType.SERVER_WRITE_LOCKED);
									client.disconnect();
								}
								System.out.println(tKey + tValue);
								lock_write = false;
								sendCommand("start_replication", predecessor);

							}

							// This command will be executed at new node
							else if (value.trim().equals("start_replication")) {
								String successor = metadata.getPredecessor(currentNode);
								// erase first successor
								sendCommand("erase_first", successor);
							}
							// erase first successor
							// This command will be executed at first successor
							// of new node
							else if (value.trim().equals("erase_first")) {
								replica_1.eraseContent();
								replica_2.eraseContent();
								String successor = metadata.getPredecessor(currentNode);
								sendCommand("erase_second_", successor);
							}
							// erase second successor
							// This command will be executed at second successor
							// of new node
							else if (value.trim().equals("erase_second_")) {
								replica_2.eraseContent();
								replica_1.eraseContent();
								String secondPredecessor = metadata.getSeSuccessor(currentNode);
								sendCommand("move_newnode_successor", secondPredecessor);

							}
							// This command will be executed at new node
							else if (value.trim().equals("move_newnode_successor")) {

								moveDataToSuccessors(currentNode, metadata.getPredecessor(currentNode),
										metadata.getSePredecessor(currentNode));
								String successor = metadata.getPredecessor(currentNode);
								sendCommand("reach_suc_1", successor);

							}

							// This command will be executed at first successor
							// of new node
							else if (value.trim().equals("reach_suc_1")) {
								String secondSuccessor = metadata.getSePredecessor(currentNode);
								sendCommand("reach_suc_2", secondSuccessor);
							}
							// This code will be executed at third successor of
							// new node
							else if (value.trim().equals("reach_suc_2")) {
								replica_2.eraseContent();
								String secondPredecessor = metadata.getSeSuccessor(currentNode);
								sendCommand("reach_pre_2", secondPredecessor);
							}

							// This code will be executed at first successor of
							// new node
							else if (value.trim().equals("reach_pre_2")) {
								moveDataToSuccessors(currentNode, metadata.getPredecessor(currentNode),
										metadata.getSePredecessor(currentNode));
								String predecessor = metadata.getSuccessor(currentNode);
								sendCommand("reach_new_node", predecessor);
							}
							// This code will be execute at new node
							else if (value.trim().equals("reach_new_node")) {
								replica_1.eraseContent();
								replica_2.eraseContent();
								sendCommand("invoke_rep_pre_2", metadata.getSeSuccessor(currentNode));
							}

							// This code will be executed at predecessor 2 of
							// new node
							else if (value.trim().equals("invoke_rep_pre_2")) {
								List<String> keyList = store.getAllKey();
								String tKey = "";
								String tValue = "";
								for (String cKey : keyList) {
									tKey = tKey + ":" + cKey;
									tValue = tValue + ":" + store.get(cKey);
								}
								String secondSuccessor = metadata.getSePredecessor(currentNode);
								if (tKey.length() > 1) {
									KVStore client = new KVStore(secondSuccessor.trim().split(":")[0],
											Integer.parseInt(secondSuccessor.trim().split(":")[1]));
									client.connect();

									moveData(tKey, tValue, client, StatusType.SERVER_REPLICA_SUC2);
									client.disconnect();
								}
								String successor = metadata.getPredecessor(currentNode);
								sendCommand("invoke_rep_pre_1", successor);
							}
							// This code will be executed at predecessor 1 of
							// new node
							else if (value.trim().equals("invoke_rep_pre_1")) {
								moveDataToSuccessors(currentNode, metadata.getPredecessor(currentNode),
										metadata.getSePredecessor(currentNode));
							}

							/*
							 * Update command is responsible for updating
							 * metadat of current node where it's being invoked
							 */
							else if (value.trim().equals("update")) {
								logger.debug("Received \t<type:" + key + "  name:" + value + " Server Status:" + status
										+ ">");
								server.setMetaData(msg.getMetadata());
								metadata = server.getMetaData();
								predecessor = metadata.getSuccessor(currentNode);
								successor = metadata.getPredecessor(currentNode);

							}

						}
						/*
						 * If no command matches as default server will be
						 * initialied
						 */
						else {
							int cachesize = Integer.parseInt(key);
							if (value.trim().equals("FIFO")) {
								cache = new LRUCache(cachesize, false);
							} else if (value.trim().equals("LRU")) {
								cache = new LRUCache(cachesize, true);
							} else if (value.trim().equals("LFU")) {
								float evictionfactor = (float) 1 / cachesize;
								cache = new LFUCache(cachesize, evictionfactor);
							}
							metadata = msg.getMetadata();
							server.setMetaData(metadata);
							server.setCache(cache);
							serverState = false;
							logger.debug("Server Initiated with \t<Cache Size:" + key + "  displacement strategy:"
									+ value + " Server Status:" + status.toString() + ">");

						}
					}
					/* Write pairs to replicas starts here */
					/************************************/
					else if (status == StatusType.SERVER_REPLICA_SUC1) {

						if (serverState == true) {

							if (lock_write == false) {

								logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
										+ status.toString() + ">");

								try {
									// Normal put operation

									String oldVal = cache.get(key);
									// previously key presents in cache
									// memory

									if (oldVal != null) {

										// update cache
										if (value.equals("null"))
											cache.put(key, null);
										else
											cache.put(key, value);
										// update replica 1
										replica_1.put(key, value);

									}
									// previously key absents in cache
									// memory

									else {
										oldVal = replica_1.get(key);

										// previously key presents in
										// file
										if (oldVal != null) {

											// update file and insert
											// into
											// cache
											replica_1.put(key, value);

											// update cache
											if (value.equals("null"))
												cache.put(key, null);
											else
												cache.put(key, value);

										}
										// key absents in file
										else {

											// update cache
											if (value.equals("null"))
												cache.put(key, null);
											else
												cache.put(key, value);
											// stores into replica 1
											replica_1.put(key, value);

										}

									}
								}

								catch (Exception e) {
									System.out.println(e.toString());
								}

							} else {
								logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
										+ "SERVER_WRITE_LOCKED Can't write to server" + ">");

							}
						} else {
							logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
									+ "SERVER_STOPPED Please issue start command again" + ">");

						}

					} else if (status == StatusType.SERVER_REPLICA_SUC2) {

						if (serverState == true) {

							if (lock_write == false) {

								try {
									// Normal put operation

									String oldVal = cache.get(key);
									// previously key presents in cache
									// memory

									if (oldVal != null) {

										// update cache
										if (value.equals("null"))
											cache.put(key, null);
										else
											cache.put(key, value);
										// update replica 2
										replica_2.put(key, value);

									}
									// previously key absents in cache
									// memory

									else {
										oldVal = replica_2.get(key);

										// previously key presents in
										// file
										if (oldVal != null) {

											// update replica 2 and insert
											// into
											// cache
											replica_2.put(key, value);

											// update cache
											if (value.equals("null"))
												cache.put(key, null);
											else
												cache.put(key, value);

										}
										// key absents in file
										else {

											// update cache
											if (value.equals("null"))
												cache.put(key, null);
											else
												cache.put(key, value);
											// store into replica 2
											replica_2.put(key, value);

										}

									}
								}

								catch (Exception e) {
									System.out.println(e.toString());
								}

							} else {
								logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
										+ "SERVER_WRITE_LOCKED Can't write to server" + ">");

							}
						} else {
							logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
									+ "SERVER_STOPPED Please issue start command again" + ">");

						}

					}
					/* Write pairs to replicas ends here */
					/************************************/

					/*
					 * Transferring data from one server to another server
					 * starts here
					 */
					/****************************************************************/
					else if (status == StatusType.SERVER_WRITE_LOCKED) {
						logger.debug(
								"Received \t<key:" + key + "  value:" + value + " Status:" + status.toString() + ">");

						if (serverState == true) {

							if (lock_write == false) {

								logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
										+ status.toString() + ">");

								try {
									// Normal put operation

									String oldVal = cache.get(key);
									// previously key presents in cache
									// memory

									if (oldVal != null) {

										// update cache
										cache.put(key, value);
										// update file
										store.put(key, value);

									}
									// previously key absents in cache
									// memory

									else {
										oldVal = store.get(key);

										// previously key presents in
										// file
										if (oldVal != null) {

											// update file and insert
											// into
											// cache
											store.put(key, value);
											cache.put(key, value);

										}
										// key absents in file
										else {

											cache.put(key, value);
											// store into file
											store.put(key, value);

										}

									}

								}

								catch (Exception e) {
									System.out.println(e.toString());
								}

							} else {
								logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
										+ "SERVER_WRITE_LOCKED Can't write to server" + ">");

							}
						} else {
							logger.debug("Received \t<key:" + key + "  value:" + value + " Status:"
									+ "SERVER_STOPPED Please issue start command again" + ">");

						}

					}
					/*
					 * Transferring data from one server to another server ends
					 * here
					 */
					/****************************************************************/
					/* Put pairs starts here */
					/***********************/
					else if (status == StatusType.PUT) {
						metadata = server.getMetaData();
						byte[] totalByte = null;
						logger.debug(
								"Received \t<key:" + key + "  value:" + value + " Status:" + status.toString() + ">");

						if (key.equals("client_:;_mac")) {
							clientMac.add(value);
						}
						if (serverState == true) {

							if (lock_write == false) {

								String[] tokens = key.split(";");

								/*
								 * First splits key of a put operation using
								 * regex (;) If the token length after splitting
								 * is larger than 1 then it's a put operation
								 * from control room that needs to be handled in
								 * different way
								 */
								if (tokens.length > 1) {

									/*
									 * Put the subscription message not only the
									 * persistent storage of current server node
									 * but also in all server node's
									 */
									if (tokens[0].equals("subscribe")) {
										totalByte = totalByteArray(tokens[0].getBytes(), value.getBytes(),
												StatusType.SUB_S.toString().getBytes(),
												SerializationUtils.serialize(null));
										sendAsync(totalByte).call();
										controlRoom.put(tokens[1], value);
										for (String node : metadata.getAll()) {
											try {
												if (!node.equals(currentNode)) {
													sendCommand("subscribe;" + tokens[1] + ";" + value, node);
												}
											} catch (Exception e) {

											}
										}

									}
									/*
									 * Discard subscription from all server
									 * nodes
									 */
									else if (tokens[0].equals("unsubscribe")) {
										totalByte = totalByteArray(tokens[0].getBytes(), value.getBytes(),
												StatusType.UNSUB_S.toString().getBytes(),
												SerializationUtils.serialize(null));
										sendAsync(totalByte).call();
										controlRoom.put(tokens[1], "null");

										for (String node : metadata.getAll()) {
											try {
												if (!node.equals(currentNode)) {
													sendCommand("unsubscribe;" + tokens[1], node);
												}
											} catch (Exception e) {

											}
										}
									}
									/*
									 * cntrl_room keyword is given in put
									 * operation from control room when it tries
									 * to connect with any server node Server
									 * node then check it's notification buffer
									 * whether there is any pending notification
									 * available for this control room
									 */
									else if (tokens[0].equals("cntrl_room")) {

										byte[] metaBytes = SerializationUtils.serialize(null);
										totalByte = totalByteArray(tokens[0].getBytes(), value.getBytes(),
												StatusType.PUT_SUCCESS.toString().getBytes(), metaBytes);
										sendAsync(totalByte).call();

										List<String> buffers = notificationBuffer.getAllKey();
										for (String buffer : buffers) {
											if (buffer.equals(value)) {
												String bufferMessage = notificationBuffer.get(buffer);
												sendUpdate(
														"Data has been changed \t<key: " + bufferMessage.split(":")[0]
																+ " value: " + bufferMessage.split(":")[1] + " >",
														buffer);
												notificationBuffer.put(buffer, "null");

												for (String node : metadata.getAll()) {
													if (!node.equals(currentNode)) {
														try {
															sendCommand("removebuffer;" + buffer, node);
														} catch (Exception e) {

														}

													}
												}
											}
										}

									}

								}
								/* Put operation from client */
								else {
									if (metadata.get(key).trim().equals(currentNode)) {

										try {

											// null value is given
											if (value.equals("null")) {

												// there are some pairs in the
												// cache
												// memory
												if (cache.size() > 0) {

													String oldVal = cache.get(key);

													// pair presents in the
													// cache
													// memory
													if (oldVal != null) {
														// remove from cache
														// memory
														cache.remove(key);
														// remove from file
														store.put(key, "null");

														byte[] metaBytes = SerializationUtils.serialize(metadata);
														totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
																StatusType.DELETE_SUCCESS.toString().getBytes(),
																metaBytes);

													}
													// Pair absents in the cache
													// memory
													else {
														oldVal = store.get(key);

														// pair exists in file
														if (oldVal != null) {

															// delete from file
															store.put(key, "null");

															byte[] metaBytes = SerializationUtils.serialize(metadata);
															totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
																	StatusType.DELETE_SUCCESS.toString().getBytes(),
																	metaBytes);
														}
														// pair absents in file
														else {

															byte[] metaBytes = SerializationUtils.serialize(metadata);
															totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
																	StatusType.DELETE_ERROR.toString().getBytes(),
																	metaBytes);

														}
													}

												}
												// No pair in the cache memory
												else {
													// since nothing in cache
													// memory
													// represents nothing in
													// file so
													// delete error
													// need some improvement
													// that is
													// checking of file also

													byte[] metaBytes = SerializationUtils.serialize(metadata);
													totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
															StatusType.DELETE_ERROR.toString().getBytes(), metaBytes);
												}
											}
											// Normal put operation
											else {
												String oldVal = cache.get(key);
												// previously key presents in
												// cache
												// memory
												if (oldVal != null) {

													System.out.println("key presents in the cache");
													// update cache
													cache.put(key, value);
													// update file
													store.put(key, value);

													byte[] metaBytes = SerializationUtils.serialize(metadata);
													totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
															StatusType.PUT_UPDATE.toString().getBytes(), metaBytes);
													sendNotification(key, value);

												}
												// previously key absents in
												// cache
												// memory

												else {
													oldVal = store.get(key);

													// previously key presents
													// in
													// file
													if (oldVal != null) {

														System.out.println("key presents in the file");
														// update file and
														// insert
														// into cache
														store.put(key, value);
														cache.put(key, value);

														byte[] metaBytes = SerializationUtils.serialize(metadata);
														totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
																StatusType.PUT_UPDATE.toString().getBytes(), metaBytes);
														sendNotification(key, value);

													}
													// key absents in file
													else {

														cache.put(key, value);
														// store into file
														store.put(key, value);

														byte[] metaBytes = SerializationUtils.serialize(metadata);
														totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
																StatusType.PUT_SUCCESS.toString().getBytes(),
																metaBytes);
													}

												}

											}
											// update
											writePairToReplicas(key, value, metadata.getPredecessor(currentNode),
													metadata.getSePredecessor(currentNode));

										} catch (Exception e) {

											byte[] metaBytes = SerializationUtils.serialize(metadata);
											totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
													StatusType.PUT_ERROR.toString().getBytes(), metaBytes);
											System.out.println(e.toString());
										}
										// finally send the message
										finally {
											sendAsync(totalByte).call();
											logger.debug("SEND \t<Key:" + key + "  value:" + value + " Status:" + status
													+ ">");

										}
									} else {
										cache.remove(key);
										store.put(key, "null");

										byte[] metaBytes = SerializationUtils.serialize(metadata);
										totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
												StatusType.SERVER_NOT_RESPONSIBLE.toString().getBytes(), metaBytes);
										sendAsync(totalByte).call();
									}
								}

							} else {
								byte[] metaBytes = SerializationUtils.serialize(metadata);
								totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
										StatusType.SERVER_WRITE_LOCKED.toString().getBytes(), metaBytes);

								sendAsync(totalByte).call();
							}
						} else {

							byte[] metaBytes = SerializationUtils.serialize(metadata);
							totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
									StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);
							sendAsync(totalByte).call();
						}
					}
					/* Put pairs ends here */
					/***********************/
					/* Get pairs starts here */
					/***********************/
					else if (status == StatusType.GET) {
						metadata = server.getMetaData();
						logger.debug(
								"Received \t<key:" + key + "  value:" + value + " Status:" + status.toString() + ">");
						byte[] totalByte = null;
						if (serverState == true) {

							if (metadata.get(key).trim().equals(currentNode)) {
								try {

									// retrieve value from cache
									String val = cache.get(key);

									// value exists in cache
									if (val != null) {
										byte[] metaBytes = SerializationUtils.serialize(metadata);
										totalByte = totalByteArray("x".getBytes(), val.getBytes(),
												StatusType.GET_SUCCESS.toString().getBytes(), metaBytes);

									}
									// value absents from cache
									else {
										// try to retrieve from file
										val = store.get(key);

										// Value in the file
										if (val != null) {
											cache.put(key, val);

											byte[] metaBytes = SerializationUtils.serialize(metadata);
											totalByte = totalByteArray("x".getBytes(), val.getBytes(),
													StatusType.GET_SUCCESS.toString().getBytes(), metaBytes);
										}
										// value is nowhere
										else {
											byte[] metaBytes = SerializationUtils.serialize(metadata);
											totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
													StatusType.GET_ERROR.toString().getBytes(), metaBytes);

										}
									}
								} catch (Exception e) {

									byte[] metaBytes = SerializationUtils.serialize(metadata);
									totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
											StatusType.GET_SUCCESS.toString().getBytes(), metaBytes);
								} finally {
									sendAsync(totalByte).call();

								}
							} else {
								cache.remove(key);
								store.put(key, "null");
								byte[] metaBytes = SerializationUtils.serialize(metadata);
								totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
										StatusType.SERVER_NOT_RESPONSIBLE.toString().getBytes(), metaBytes);

								sendAsync(totalByte).call();
							}
						} else {
							byte[] metaBytes = SerializationUtils.serialize(metadata);
							totalByte = totalByteArray("x".getBytes(), "x".getBytes(),
									StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);

							sendAsync(totalByte).call();
						}

					}
					/* Get pairs ends here */
					/***********************/

				} catch (NullPointerException | NegativeArraySizeException | IOException | InterruptedException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}

		} catch (

		IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			// finally closes everything
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			} catch (NullPointerException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void shutdown() throws IOException {
		clientSocket.close();
		input.close();
		output.close();
		System.exit(0);
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * 
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	public void sendMessage(String conMsg) throws IOException {
		byte[] msgBytes = conMsg.getBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + ">: '"
				+ conMsg + "'");
	}

	/**
	 * Method sends KVMessage object by marshalling it into bytes array
	 * 
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	public void send(KVMessage message) throws InterruptedException, IOException {
		// System.out.println("Sent byte length "+ message.getBytes().length);
		output.write(message.getBytes(), 0, message.getBytes().length);
		output.flush();
		logger.debug("SEND \t<Key:" + message.getKey() + "  value:" + message.getValue() + " Status:"
				+ message.getStatus() + ">");

	}

	/**
	 * Method sends array of bytes asychronously
	 * 
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	public Callable<Boolean> sendAsync(byte[] totalBytes) throws InterruptedException, IOException {

		Callable<Boolean> callable = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {

				output.write(totalBytes, 0, totalBytes.length);
				output.flush();
				return true;
			}
		};
		return callable;
	}

	/**
	 * Receives byte array from input stream Divides into parts by reading the
	 * header bits Finally performs unmarshalling on each part
	 * 
	 * @return {@link Callable}
	 */
	private Callable<KVMessage> receiveDataAsyn() throws InterruptedException, IOException {

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
							// System.out.println(totalValueByte);
							byte[] valueByte = new byte[totalValueByte];
							System.arraycopy(receiveTotal, keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1,
									valueByte, 0, valueByte.length);
							// System.out.println(new String(valueByte));

							int statusSlotLength = receiveTotal[keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1
									+ valueByte.length];
							byte[] statusByte = new byte[statusSlotLength];
							System.arraycopy(receiveTotal,
									keyLengthSlot + 1 + keyByte.length + valueLengthSlot + 1 + valueByte.length + 1,
									statusByte, 0, statusByte.length);
							// System.out.println(statusSlotLength);
							// System.out.println(new String(statusByte));
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
							logger.warn("No value");
						}
					} else {
						logger.warn("No key");
					}

				} else {
					logger.warn("No data");
				}

				return message;

			}
		};
		return callable;

	}

	/**
	 * Given a byte array unmarshal into string Returns status matches with
	 * string
	 * 
	 * @return {@link StatusType}
	 * @param array
	 *            {@link BYTE_ARRAY}
	 */
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
		} else if (strStatus.trim().equals("SERVER_REPLICA_SUC1")) {
			status = StatusType.SERVER_REPLICA_SUC1;
		} else if (strStatus.trim().equals("SERVER_REPLICA_SUC2")) {
			status = StatusType.SERVER_REPLICA_SUC2;
		}
		return status;

	}

}