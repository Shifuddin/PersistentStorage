/**
	 * Server administration class
	 * Launch servers by reading from ecs.config file
	 * Add new server node
	 * Remove server node
	 * Automatically add new server node after failure is detected
	 * */
package app_kvEcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.SerializationUtils;

import common.messages.KVConsistentHash;
import common.messages.KVHashFunction;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImp;

public class ECS {

	private String config = null;
	private List<String> serverRepository;
	private KVConsistentHash<String> metadata;
	private int numberOfNodes;
	private List<ECSClientSocket> ecsClientSocket;
	private int confState = 0;
	private String node = "";
	private String faultNodeGlobal = "";
	private AtomicInteger count = new AtomicInteger(0);

	
	public ECS(String config) throws FileNotFoundException {
		this.config = config;

	}

	/**
	 * Reads ecs.config file and add server address to list 
	 * Number of nodes given in initialization command used as a loop counter to exit
	 * @return List of server address*/
	private List<String> readRepository() throws FileNotFoundException {
		serverRepository = new ArrayList<String>();
		if (config != null) {
			/*Check the file name*/
			if (config.trim().equals("ecs.config")) {
				@SuppressWarnings("resource")
				Scanner sc = new Scanner(new File("ecs.config"));
				String[] array;
				int number = 0;
				/*Read and add*/
				while (sc.hasNextLine() && number < numberOfNodes) {
					array = sc.nextLine().split(" ");
					serverRepository.add(array[1] + ":" + array[2]);
					number++;
				}
				return serverRepository;

			}
		} else {
			System.out.println("No configuration file");
		}
		return null;

	}

	/**
	 * Initialize servers with cache size and displacement strategy
	 * 
	 * @param numberOfNodes
	 * @param cacheSize
	 * @param displacementStrategy
	 * @return Integer
	 * @throws InterruptedException 
	 */
	public int initService(int numberOfNodes, int cacheSize, String displacementStrategy) throws InterruptedException {
		/*Check whether initialization command typed or not*/
		if (confState == 0) {
			
			/*Match with supported displacement strategies*/
			if (displacementStrategy.equals("FIFO") || displacementStrategy.equals("LRU")
					|| displacementStrategy.equals("LFU")) {
				this.numberOfNodes = numberOfNodes;

				try {
					// reads ecs.config file
					serverRepository = readRepository();
					ecsClientSocket = new ArrayList<ECSClientSocket>();

					String[] tokens;
					String rmIndex = "";
					/*
					 * run each server and add client connection to client socket
					 * list
					 */
					for (int i = 0; i < serverRepository.size(); i++) {
						tokens = serverRepository.get(i).split(":");
						Runtime.getRuntime().exec("cmd /c start java -jar C:\\Users\\shifuddin\\Desktop\\my"
								+ String.valueOf(i + 1) + "\\ms5-server.jar " + tokens[1]);
						ecsClientSocket.add(new ECSClientSocket(Integer.parseInt(tokens[1]), tokens[0]));

						/*
						 * Connect to each server and receives connection string
						 */
						try {
							ecsClientSocket.get(i).connect();
							ecsClientSocket.get(i).receiveConnectionMsg();

						} catch (Exception e) {
							// TODO: handle exception
							System.out.println("Unable to connect with " + tokens[0] + ":" + tokens[1]);
							// ecsClientSocket.remove(i);
							rmIndex = rmIndex + ":" + String.valueOf(i);
						}
					}
					tokens = rmIndex.split(":");

					/*
					 * Removes client sockets using those connection to server
					 * were not possible
					 */
					if (tokens.length > 1) {
						Iterator<ECSClientSocket> itEcs = ecsClientSocket.iterator();
						for (int i = 1; i < tokens.length; i++) {
							ECSClientSocket s = itEcs.next();
							itEcs.remove();
						}
						Iterator<String> itRep = serverRepository.iterator();
						for (int i = 1; i < tokens.length; i++) {
							String s = itRep.next();
							itRep.remove();
						}
					}
					/*
					 * Calculate metadata and send it alongside with cache size
					 * and displacement strategy to each server
					 */
					metadata = new KVConsistentHash<>(new KVHashFunction(), serverRepository, serverRepository.size());
					byte [] totalByte = totalByteArray(String.valueOf(cacheSize).getBytes(), displacementStrategy.getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
					for (int i = 0; i < ecsClientSocket.size(); i++) {
						ecsClientSocket.get(i).send(totalByte);

					}
					confState = 1;

					return 0;

				} catch (IOException ioe) {
					System.out.println(ioe.toString());
					return -1;
				}
			} else {
				return -2;
			}

		} else if (confState == 1)
			return 1;
		else if (confState == 2)
			return 2;
		else
			return 3;

	}

	/**
	 * After certain interval checks each client socket
	 * If any of the available server sends information regarding 
	 * any fault node counter increases
	 * If counter raises to certain level and that node existed previously 
	 * in the metadata of nodes, data are shifted to a newly added node from the server
	 * which holds replicated data of down node
	 * @return Void
	 * @throws IOException, InterruptedException
	 * */
	public void checkFaulNode() throws IOException, InterruptedException {
		/*Checks each client socket in different threads*/
		for (ECSClientSocket ecs : ecsClientSocket) {
			(new Thread() {
				public void run() {
					// do stuff
					try {
						String faultNode = ecs.receiveMsg();
						if (faultNode != null) {
							count.incrementAndGet();
							faultNodeGlobal = faultNode;
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}).start();

		}

		/*We used atomic integer so that each thread can access it to increase counter
		 *Checks the counter and faultnode address received from client sockets*/
		if (count.intValue() >= 1 && !metadata.getPredecessor(faultNodeGlobal).equals("not found")) {
			count.set(0);

			int position = serverRepository.indexOf(faultNodeGlobal);

			String successor = metadata.getPredecessor(faultNodeGlobal);
			faultNodeGlobal = "";
			
			/* Removes fault node from server repository and corresponding client socket from 
			 * client socket list*/
			serverRepository.remove(position);
			ecsClientSocket.remove(position);
			
			/*Calculate new metadata*/
			metadata = new KVConsistentHash<>(new KVHashFunction(), serverRepository, serverRepository.size());

			/*Sends updated metadata to all existing servers through update command*/
			byte [] totalByte = totalByteArray("command".getBytes(), "update".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
			for (int i = 0; i < ecsClientSocket.size(); i++) {
				ecsClientSocket.get(i).send(totalByte);

			}

			/*Finds position of the successor node of fault node from server list and sends transfer command
			 * this command tells the successor to start data acquisition and replication again having -1 number of
			 * nodes */
			position = serverRepository.indexOf(successor);
			totalByte = totalByteArray("command".getBytes(), "transfer".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
			ecsClientSocket.get(position).send(totalByte);
			/*Finally adds a replacement server*/
			addNode(4, "FIFO");

		}
		/*If we find any node information from servers which does not exist in the metadata
		 * we just set the counter to zero*/
		if (metadata.getPredecessor(faultNodeGlobal).equals("not found"))
			count.set(0);
	}

	/**
	 * Start initialized servers
	 * 
	 */
	public String start() {
		if (confState == 1) {

			if (ecsClientSocket.size() > 0) {
				/*Send start command to all servers to ready them to accept key to write
				 * otherwise servers will send back write lock message*/
				byte [] metaBytes = SerializationUtils.serialize(null);
				byte [] totalByte = totalByteArray("command".getBytes(), "start".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);
				
				for (int i = 0; i < ecsClientSocket.size(); i++) {
					try {
						ecsClientSocket.get(i).send(totalByte);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				confState = 2;
				/*Create a runnable and execute it after some interval using schedule executor service*/
				Runnable checkFault = new Runnable() {
					public void run() {
						try {
							checkFaulNode();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
				exec.scheduleAtFixedRate(checkFault, 0, 80, TimeUnit.SECONDS);
				return "Service Started";
			} else {
				return "Nothing to start";
			}
		} else if (confState == 2)
			return "Service already started. Stop First";
		else if (confState == 0)
			return "Please Initiated service first";
		return "Unknown state";

	}

	/**
	 * Stop initialized servers by sending stop command to each server
	 * @return String
	 * 
	 */
	public String stop() {
		if (confState == 2) {

			if (ecsClientSocket.size() > 0) {
				
				byte [] metaBytes = SerializationUtils.serialize(null);
				byte [] totalByte = totalByteArray("command".getBytes(), "stop".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);
				
				for (int i = 0; i < ecsClientSocket.size(); i++) {
					try {
						ecsClientSocket.get(i).send(totalByte);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				confState = 1;
				return "Service Stopped";
			} else
				return "Nothing to stop";
		} else if (confState == 1)
			return "Start service first";
		else if (confState == 0)
			return "Service not initiated";
		return "Unknown state";

	}

	/**
	 * Shutdown all servers by sending shutdown command to each server node
	 * @return String
	 */
	public String shutdown() {
		if (confState == 1 || confState == 2 || confState == 0) {

			if (ecsClientSocket.size() > 0) {
				byte [] metaBytes = SerializationUtils.serialize(null);
				byte [] totalBytes = totalByteArray("command".getBytes(), "shutdown".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), metaBytes);
				for (int i = 0; i < ecsClientSocket.size(); i++) {
					try {
						ecsClientSocket.get(i).send(totalBytes);
					} catch (IOException e) {
					
						e.printStackTrace();
					}
				}
				confState = 0;
				return "Stop nodes & Service exits";
			} else {
				return "Service exits";
			}
		} else

			return "Failed to exit service";

	}

	/**
	 * Add a new node with cache size and displacement strategy
	 * 
	 * @param cacheSize
	 * @param displacementStrategy
	 * @return String
	 */
	public String addNode(int cacheSize, String displacementStrategy)
			throws UnknownHostException, IOException, InterruptedException {
		
		/*Check whether servers are initialized or started*/
		if (confState == 1 || confState == 2) {

			/*Check supported displacement strategy*/
			if (displacementStrategy.equals("FIFO") || displacementStrategy.equals("LFU")
					|| displacementStrategy.equals("LRU")) {
				if (config != null) {
					if (config.trim().equals("ecs.config")) {
						@SuppressWarnings("resource")
						Scanner sc = new Scanner(new File("ecs.config"));
						String[] array;

						/*Find new server address from config file which 
						 * are not already in server list*/
						while (sc.hasNextLine()) {
							array = sc.nextLine().split(" ");
							node = array[1] + ":" + array[2];
							if (!serverRepository.contains(node))
								break;

						}

						if (node == "") {
							return "No Idle node found";
						} else {

							/*Add found node to server list*/
							serverRepository.add(node);
							
							/*Calculate new metadata from new server list*/
							metadata = new KVConsistentHash<>(new KVHashFunction(), serverRepository,
									serverRepository.size());

							/*Run the server by invoking jar file*/
							String[] tokens = node.split(":");
							Runtime.getRuntime()
									.exec("cmd /c start java -jar C:\\Users\\shifuddin\\Desktop\\my2//ms5-server.jar "
											+ tokens[1]);

							/*Create client socket to connect with new server*/
							ECSClientSocket ecs = new ECSClientSocket(Integer.parseInt(tokens[1]), tokens[0]);
							/*Connect and receive connection string*/
							ecs.connect();
							ecs.receiveConnectionMsg();
							
							
							
							/*Initialize server with cache size, displacement strategy and new metadata*/
							byte [] totalByte = totalByteArray(String.valueOf(cacheSize).getBytes(), displacementStrategy.getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
							ecs.send(totalByte);

							/*Send updated metadata to all servers except new server because already give to that server*/
							totalByte = totalByteArray("command".getBytes(), "update".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
							for (int i = 0; i < ecsClientSocket.size(); i++) {
								ecsClientSocket.get(i).send(totalByte);

							}

							/*Send add command to new server to start the replication process*/
							totalByte = totalByteArray("command".getBytes(), "add_new_node".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
							ecs.send(totalByte);
							ecsClientSocket.add(ecs);
							numberOfNodes++;
							return "Successfully Added "+ node;

						}

					} else
						return "No conf file";
				} else {
					return "No conf file";
				}

			} else {
				return "Displacement Strategy not supported";
			}
		} else
			return "Initialize the service first";

	}

	/**
	 * Remove a arbitrary node/server from the ring
	 * @return String removed node
	 */
	// updated
	public String removeNode() throws IOException, InterruptedException {
		
		/*Check whether servers are initialized or started*/
		if (confState == 1 || confState == 2) {
			
			/*Is there enough servers to remove one*/
			if (ecsClientSocket.size() > 0) {
				Random r = new Random();
				// int position = r.nextInt(ecsClientSocket.size());
				int position = 1;
				
				/*Node to be removed*/
				node = serverRepository.get(position);

				/*Successor and predecessor of to be removed node*/
				String successor = metadata.getPredecessor(node);
				
				/*Find client socket of targeted node*/
				ECSClientSocket ecs = ecsClientSocket.get(position);
				/*Remove target node from server list and client socket list*/
				serverRepository.remove(position);
				ecsClientSocket.remove(position);
				/*Calculate new metadata from new server list*/
				metadata = new KVConsistentHash<>(new KVHashFunction(), serverRepository, serverRepository.size());

				/*Send update command to all servers except target server*/
				byte [] totalByte = totalByteArray("command".getBytes(), "update".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
				for (int i = 0; i < ecsClientSocket.size(); i++) {
					ecsClientSocket.get(i).send(totalByte);

				}
				
				/*Send transfer command to the successor to start transferring of data from target node to successor*/
				position = serverRepository.indexOf(successor);
				totalByte = totalByteArray("command".getBytes(), "transfer".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
				ecsClientSocket.get(position).send(totalByte);
				
				/*Shutdown target node*/
				totalByte = totalByteArray("command".getBytes(), "shutdown".getBytes(), StatusType.SERVER_STOPPED.toString().getBytes(), SerializationUtils.serialize( metadata));
				ecs.send(totalByte);
				return "Successfully Removed node " + node;
			} else
				return "No node to remove";

		} else
			return "First Initialize service";

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
		for (int i = 0; i <= keyByteSlotCount; i++) {
			if (i == 0)
				totalByte[i] = (byte) keyByteSlotCount;
			else if (i == keyByteSlotCount)
				totalByte[i] = (byte) keyByteTotal;
			else
				totalByte[i] = 127;
		}
		System.arraycopy(keyBytes, 0, totalByte, keyByteSlotCount + 1, keyBytes.length);

		for (int i = 0; i <= valueByteSlotCount; i++) {
			if (i == 0)
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = (byte) valueByteSlotCount;
			else if (i == valueByteSlotCount)
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = (byte) valueByteTotal;
			else
				totalByte[keyByteSlotCount + 1 + keyBytes.length + i] = 127;
		}
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
