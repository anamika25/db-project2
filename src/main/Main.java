package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import parser.Parser;
import parser.ParserException;
import parser.StatementNode;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class Main {

	private static SchemaManager schemaManager;
	private static Disk disk;
	private static MainMemory memory;
	private static AbstractExecutor executor;

	public static void main(String[] args) {

		Options options = new Options();
		options.addOption("s", true, "TinySQL query");
		options.addOption("f", true, "File with TinySQL queries");

		CommandLineParser parser = new BasicParser();
		String query = null, fileName = null;

		memory = new MainMemory();
		disk = new Disk();
		schemaManager = new SchemaManager(memory, disk);
		executor = new AbstractExecutor();
		try {
			CommandLine cmd = parser.parse(options, args);
			if (cmd.hasOption("s")) {
				query = cmd.getOptionValue("s");
				System.out.println("Query: " + query);
				StatementNode parseTree = Parser.startParse(query);
				System.out.println(parseTree + "\n");
				executor.execute(parseTree, schemaManager, disk, memory);
			} else if (cmd.hasOption("f")) {
				fileName = cmd.getOptionValue("f");
				System.out.println("File to process: " + fileName);
				processFile(fileName);
			} else {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("program", options);
			}
		} catch (ParseException e) {
			System.out.println("Exception while reading command line arguments");
			e.printStackTrace();
			System.exit(0);
		} catch (ParserException e) {
			e.printStackTrace();
			System.exit(0);
		}

	}

	private static void processFile(String fileName) throws ParserException {
		BufferedReader br1 = null;
		try {
			br1 = new BufferedReader(new FileReader(fileName));
			String line = null;
			while ((line = br1.readLine()) != null) {
				line = line.trim();
				if (line.isEmpty()) {
					continue;
				}
				executor.execute(Parser.startParse(line), schemaManager, disk, memory);
			}
		} catch (FileNotFoundException e) {
			System.out.println("File not found. Exiting !!");
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			System.out.println("Error while reading file. Exiting !!");
			e.printStackTrace();
			System.exit(0);
		}

	}

}
