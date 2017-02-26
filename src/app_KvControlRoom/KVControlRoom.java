/**
 * Control room class
 * Connects to a particular server node
 * Subscribe or unsubscribe 
 * Create listener and waits for any update until stopped*/

package app_KvControlRoom;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import client.KVStore;
import common.messages.KVMessage;
import logger.LogSetup;

public class KVControlRoom {
	private Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "EchoControlRoom> ";
	private BufferedReader stdin;
	private KVStore client;
	private boolean stop = false;
	private boolean clientConnected = false;
	private String serverAddress;
	private int serverPort;
	private int port;
	private ListenerThread listenerThread;
	private boolean isRunning = true;
	
	private int getListenerPort() throws FileNotFoundException
	{
		
		Scanner sc = new Scanner(new File("controlroom.config"));
		port = sc.nextInt();
		return 1;
	}

	/**
	 * Loop for reading/writing shell commands Exits loop when quitting or
	 * having errors thrown
	 * 
	 * @throws SocketException
	 * @throws UnknownHostException
	 */
	public void run() throws UnknownHostException, SocketException {

		try {
			getListenerPort();
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
		} catch (Exception e) {
			logger.error(e);
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
			logger.info("Control room listener stoped");
			System.out.println(PROMPT + "Application exit!");
			isRunning = false;
			System.exit(0);
		} 
		
		/*Connects to particular server node and starts listener*/
		else if (tokens[0].equals("connect")) {
			if (tokens.length == 3) {
				try {
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverPort, serverAddress);
					handleNewMessage(client.getConSucString());
					client.put("cntrl_room;cntrl", "localhost:" + port);
					listenerThread = new ListenerThread();
					listenerThread.start();
					logger.info("Connected to " + serverAddress + " on port " + serverPort);
					logger.info("Control room listener started...");

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

		} 
		/*Subscribe to particular level*/
		else if (tokens[0].equals("subscribe")) {
			if (tokens.length == 2) {
				if (client != null && isClientConnected()) {
					try {

						if (tokens[1].equals("level1") || tokens[1].equals("level2") || tokens[1].equals("level3")) {

							KVMessage message = client.put(tokens[0] + ";" + "localhost:" + port, tokens[1]);
							logger.debug("Subscribe to \t " + " level:" + message.getValue() + " Status:"
									+ message.getStatus() + ">");

						} else {
							logger.warn("Invalid level");
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						logger.error("Error: \t Can't put pair-" + e.getMessage());
					}

				} else {
					printError("Not connected!");
					logger.error("Error: \t Not Connected");
				}
			} else {
				printError("No message passed!");
				logger.error("Error: \t No message Passed. Invalid number of parameters");
			}

		}
		/*Unsubscribe from any level*/
		else if (tokens[0].equals("unsubscribe")) {
			if (tokens.length == 1) {
				if (client != null && isClientConnected()) {
					try {
						KVMessage message = client.put(tokens[0] + ";" + "localhost:" + port, "x");
						logger.debug("Unsubscribe from \t " + " level:" + message.getValue() + " Status:"
								+ message.getStatus() + ">");

					} catch (Exception e) {
						logger.error("Error: \t Can't put pair-" + e.getMessage());
					}

				} else {
					printError("Not connected!");
					logger.error("Error: \t Not Connected");
				}
			} else {
				printError("No message passed!");
				logger.error("Error: \t No message Passed. Invalid number of parameters");
			}

		} 
		/*Disconnects connection from server*/
		else if (tokens[0].equals("disconnect")) {
			if (isClientConnected())
				client.disconnect();
			else
				printError("Connect first");

		} 
		/*Sets level of log*/
		else if (tokens[0].equals("logLevel")) {
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

		} 
		/*Show help message*/
		else if (tokens[0].equals("help")) {
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
		sb.append(PROMPT).append("ECHO CONTROL ROOM HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t\t establishes a connection to a server\n");
		sb.append(PROMPT).append("subscribe <level>");
		sb.append("\t\t subscribe client to particular level \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("supported level: level1, level2, level3  \n");
		sb.append(PROMPT).append("unsubscribe");
		sb.append("\t\t\t Unsubscribe client from subscribed level \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t\t exits the program");
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

	public static void main(String[] args) throws IOException {
		/* Sets log and it's level */
		new LogSetup("logs/controlroom/controlroom.log", Level.ALL);
		KVControlRoom controlRoom = new KVControlRoom();
		controlRoom.run();
	}

	/**
	 * Listener thread waits for the update messages from server for a
	 * particular level Each control room has a listener This class creates a
	 * server socket and wait for the connection to accept on a particular port
	 * If connection accepted, it then starts a new thread for that connection
	 * Multiple connection can be established
	 */
	class ListenerThread extends Thread {
		private ServerSocket serverSocket;
		private Socket client = null;

		/**
		 * Overloaded function of parent Thread class Waits for connection and
		 * runs a new thread for a new connection
		 */
		public void run() {
			try {
				serverSocket = new ServerSocket(port);

				while (isRunning) {

					if (serverSocket != null) {
						client = serverSocket.accept();
						new ServerConnection(client).start();
					}
				}

			} catch (IOException e) {

				logger.error(e);
			}
		}
	}
}
