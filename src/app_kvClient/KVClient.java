/**
 * app_kvClient shell class
 * @author Md Shiffudin Al Masud,Vidrashku Ilya, SHYAMSUNDAR DEBSARKAR
 * @version 1.1
 * 
 */
package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;
import common.messages.KVConsistentHash;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import logger.LogSetup;

public class KVClient {
	private Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "EchoClient> ";
	private BufferedReader stdin;
	private KVStore client;
	private boolean stop = false;
	private boolean clientConnected = false;
	private String serverAddress;
	private int serverPort;
	private KVConsistentHash<String> metadata = null;

	/**
	 * Loop for reading/writing shell commands Exits loop when quitting or
	 * having errors thrown
	 * 
	 * @throws SocketException
	 * @throws UnknownHostException
	 */
	public void run() throws UnknownHostException, SocketException {
		while (!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}


	private void connect(int port, String address) throws IllegalArgumentException, IOException {
		client = new KVStore(address, port);
		client.connect();
		clientConnected = true;
	}

	private void disconnect() {
		client.disconnect();
	}

	/**
	 * Handles shell input, dividing it into tokens, called from the run() loop.
	 * 
	 * @param cmdLine
	 *            command input
	 * @throws IOException
	 *             any System IO exceptions
	 */
	private void handleCommand(String cmdLine) throws IOException {
		String[] tokens = cmdLine.split(" ");
		String[] parts;

		if (tokens[0].equals("quit")) {
			stop = true;
			if (isClientConnected() == true)
				client.disconnect();
			System.out.println(PROMPT + "Application exit!");

		}
		else if (tokens[0].equals("snd")) {
			client.sendMessage(tokens[1] + '\r');

		}else if (tokens[0].equals("connect")) {
			if (tokens.length == 3) {
				try {
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverPort, serverAddress);
					handleNewMessage(client.getConSucString());
					logger.info("Connected to " + serverAddress + " on port " + serverPort);

				} catch (NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					printError(e.getMessage());
					logger.warn("Error: " + e.getMessage());
				}
			} else {
				printError("Invalid number of parameters!");
				logger.warn("Invalid number of parameters!");
			}

		} else if (tokens[0].equals("put")) {
			if (tokens.length == 3) {
				if (client != null && isClientConnected()) {

					try {

						if (metadata != null) {

							String address = metadata.get(tokens[1]);
							parts = address.split(":");

							if (serverAddress.equals("localhost"))
								serverAddress = "127.0.0.1";
							if (!address.trim().equals(serverAddress + ":" + serverPort)) {
								serverAddress = parts[0];
								serverPort = Integer.parseInt(parts[1]);
								long millis = System.currentTimeMillis() % 1000;
								disconnect();
								//clientThread.setClientSocket(false);
								connect(serverPort, serverAddress);
								System.out.println("Reconnection " +(System.currentTimeMillis() % 1000 -millis));
								logger.info(
										"Reconnected: \t<address: " + serverAddress + "\tport: " + serverPort + ">");

							}
						}
						
						logger.debug("SEND \t<Key:" + tokens[1] + "  value:" + tokens[2] + " Status:" + StatusType.PUT
								+ ">");
						//long millis = System.currentTimeMillis() % 1000;
						System.out.println(System.currentTimeMillis() % 1000);
						KVMessage message = client.put(tokens[1], tokens[2]);
						//System.out.println("ATS " +(System.currentTimeMillis() % 1000 -millis));
						logger.debug("Receive \t<Key:" + tokens[1] + "  value:" + tokens[2] + " Status:"
								+ message.getStatus() + ">" );
						//System.out.println("Size " +tokens[1].getBytes().length+tokens[2].getBytes().length+ " bytes");
						metadata = message.getMetadata();

					} catch (Exception e) {
						// TODO Auto-generated catch block
						logger.error("Error: \t Can't put pair-" + e.toString());
					}

				} else {
					printError("Not connected!");
					logger.error("Error: \t Not Connected");
				}
			} else {
				printError("No message passed!");
				logger.error("Error: \t No message Passed. Invalid number of parameters");
			}

		} else if (tokens[0].equals("get")) {
			if (tokens.length == 2) {
				if (client != null && isClientConnected()) {

					try {

						if (metadata != null) {
							String address = metadata.get(tokens[1]);
							parts = address.split(":");
							if (serverAddress.equals("localhost"))
								serverAddress = "127.0.0.1";
							if (!address.trim().equals(serverAddress + ":" + serverPort)) {
								serverAddress = parts[0];
								serverPort = Integer.parseInt(parts[1]);
								long millis = System.currentTimeMillis() % 1000;
								disconnect();
								//clientThread.setClientSocket(false);
								connect(serverPort, serverAddress);
								System.out.println("Reconnection " +(System.currentTimeMillis() % 1000 -millis));
								logger.info(
										"Reconnected: \t<address: " + serverAddress + "\tport: " + serverPort + ">");

							}
						}

						logger.debug("SEND \t<Key:" + tokens[1] + "  Status:" + StatusType.GET + ">");
						long millis = System.currentTimeMillis() % 1000;
						KVMessage message = client.get(tokens[1]);
						System.out.println("ATR " +(System.currentTimeMillis() % 1000 -millis));
						logger.debug("Receive \t<Key:" + tokens[1] + "  value:" + message.getValue()
						+ "  Status:" + message.getStatus() + ">");
						metadata = message.getMetadata();
						


					} catch (Exception e) {

						if (metadata == null)
							logger.error("Your connected server is closed. Please reconnect with another server");
						else {
							String predecessor = "No predecessor";
							try {
								predecessor = metadata.getSuccessor(serverAddress + ":" + serverPort);
								parts = predecessor.split(":");

								serverAddress = parts[0];
								serverPort = Integer.parseInt(parts[1]);

								disconnect();
							
								connect(serverPort, serverAddress);
								logger.info(
										"Reconnected: \t<address: " + serverAddress + "\tport: " + serverPort + ">");

								KVMessage message = client.get(tokens[1]);
								metadata = message.getMetadata();
								logger.debug("SEND \t<Key:" + tokens[1] + "  Status:" + StatusType.GET + ">");
								logger.debug("Receive \t<Key:" + message.getKey() + "  value:" + message.getValue()
										+ "  Status:" + message.getStatus() + ">");
							} catch (Exception ef) {
								logger.info("Can't connect with predecessor " + predecessor + " " + ef.getMessage());
							}
						}
					}

				} else {
					printError("Not connected!");
					logger.error("Error: \t Not Connected");
				}
			} else {
				printError("No message passed!");
				logger.error("Error: \t No message Passed. Invalid number of parameters");
			}

		}  else if (tokens[0].equals("disconnect")) {
			if (isClientConnected())
				client.disconnect();
			else
				printError("Connect first");

		} else if (tokens[0].equals("logLevel")) {
			if (tokens.length == 2) {

				String level = setLevel(tokens[1]);
				if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					logger.error("Error: \t No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + "Log level changed to level " + level);
				}

			} else {
				printError("Invalid number of parameters!");
				logger.error("Error: \t Invalid number of parameters!");
			}

		} else if (tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			logger.error("Error: \t Unknown command!");
			printHelp();
		}
	}

	/**
	 * Basic connection check
	 * 
	 * @return clientConnected - boolean unit, changes with "connect" and
	 *         "disconnect" commands
	 */
	public boolean isClientConnected() {
		return clientConnected;
	}

	/**
	 * Error output with shell prompt
	 * 
	 * @param error
	 *            thrown message
	 */
	private void printError(String error) {
		System.out.println(PROMPT + "Error! " + error);
	}

	/**
	 * Prints all commands and their respective variables, called is command was
	 * unknown, or if it was "help"
	 */
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t puts a key value pair to storage \n");
		sb.append(PROMPT).append("put <key> <null>");
		sb.append("\t\t deletes specified key \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t\t returns already stored value \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	/**
	 * Prints variables for command logLevel if this command was called with
	 * wrong log level
	 */
	private void printPossibleLogLevels() {
		System.out.println(PROMPT + "Possible log levels are:");
		System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	/**
	 * Called on connection to the server
	 * 
	 * @param msg
	 *            - received message
	 */
	public void handleNewMessage(String msg) {

		if (!stop) {

			System.out.println(PROMPT + msg);

		}
	}

	/**
	 * Sets the logger level
	 * 
	 * @param levelString
	 *            - specified level, sent as a token after logLevel command
	 * @return Level change result message
	 */
	private String setLevel(String levelString) {

		if (levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if (levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if (levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if (levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if (levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if (levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if (levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	/**
	 * Main entry point, Creation of the shell app, setting it to run() the
	 * loop.
	 * 
	 * @param args
	 *            does basically nothing.
	 * @throws IOException
	 *             base IO exceptions.
	 */
	public static void main(String[] args) throws IOException {
		new LogSetup("logs/client/client.log", Level.ALL);
		KVClient clientApp = new KVClient();
		clientApp.run();
	}

}

