/**
 * Stores and retrieves key value pair from file
 * @author shifuddin, Ilya*/
package app_kvServer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;

public class DBFile implements DBFileInterface{
	private Scanner in;
	private BufferedWriter out;
	private LinkedHashMap<String, String> pair;
	private String jarDir;
	private String name;
	public DBFile(String jarDir, String name)
	{
		this.jarDir = jarDir;
		this.name = name;
		try {
			File file = new File(jarDir+"/"+name+".txt");
			file.createNewFile();
			in = new Scanner(file);
			out = new BufferedWriter(new FileWriter(jarDir + "/" + name + ".txt", true));
			load();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**Loads all key value pairs into a {@link LinkedHashMap}
	 * @return {@link Void}
	 * */
	private void load()
	{
		String [] line;
		pair = new LinkedHashMap<String, String>();
		while (in.hasNextLine())
		{
			line =in.nextLine().split("=");
			pair.put(line[0], line[1]);
		}
		
	}
	public List<String> getAllKey()
	{
		List<String> lines = new ArrayList<String>();
		try {
			in = new Scanner(new File (jarDir+"/" + name+".txt"));
			String [] tokens;
			while (in.hasNextLine())
			{
				tokens = in.nextLine().split("=");
				lines.add(tokens[0]);
			}
		}
		catch(FileNotFoundException f)
		{
			System.out.println(f.toString());
		}
		return lines;
	}
	public String get(String key)
	{
	
		return pair.get(key);
	}
	public void put(String key, String value)
	{
		if (value.equals("null"))
		{
			String oldValue;
			if ((oldValue = pair.get(key)) != null)
			{
				pair.remove(key);
				remove(key + "=" + oldValue);
			}
			else
			{
				System.out.println("Can't delete because no previous pair exists");
			}
					
		}
		else
		{
			
			String oldValue;
			if ((oldValue = pair.get(key)) == null) {
				pair.put(key, value);
				append(key, value);
				
			} else {
				pair.put(key, value);
				replace(key + "=" + oldValue, key + "=" + value);
				
			}
		}
	}
	/**Appends pairs into file
	 * @param key {@link String}
	 * @param value {@link String}
	 * */
	private void append(String key, String value)
	{
		try {
			
			out.write(key + "="+value +"\n");
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**Replace old value by new value
	 * @param oldValue {@link String}
	 * @param value {@link String}
	 * */
	private void replace(String oldValue, String value)
	{
		Path path = Paths.get(jarDir+"/" +name+".txt");
		Charset charset = StandardCharsets.UTF_8;

		String content = null;
		try {
			content = new String(Files.readAllBytes(path), charset);
		} catch (IOException e) {
			e.printStackTrace();
		}
		content = content.replaceAll(oldValue, value);
		try {
			Files.write(path, content.getBytes(charset));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**Removes a particular pair
	 * @param key {@link String}
	 * @return Void
	 * */
	private void remove(String key)
	{
		List<String> lines = new ArrayList<String>();
		try {
			in = new Scanner(new File (jarDir+"/"+name+".txt"));
			while (in.hasNextLine())
			{
				lines.add(in.nextLine());
			}
			lines.remove(key);
			
			Path path = Paths.get(jarDir+"/"+name +".txt");
			Charset charset = StandardCharsets.UTF_8;
			Files.write(path, lines, charset);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void eraseContent() throws IOException
	{
		out = new BufferedWriter(new FileWriter(jarDir + "/" + name + ".txt", false));
		out.write("");
		out.flush();
		out = new BufferedWriter(new FileWriter(jarDir + "/" + name + ".txt", true));
		pair.clear();
	}
}
