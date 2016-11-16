package main;

import parser.StatementNode;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class ExecutionParameter {

	private StatementNode parseTreeRoot;
	private SchemaManager schemaManager;
	private MainMemory memory;
	private Disk disk;

	public ExecutionParameter(StatementNode parseTreeRoot, SchemaManager schemaManager, MainMemory memory, Disk disk) {
		this.parseTreeRoot = parseTreeRoot;
		this.schemaManager = schemaManager;
		this.memory = memory;
		this.disk = disk;
	}

	public StatementNode getParseTreeRoot() {
		return parseTreeRoot;
	}

	public void setParseTreeRoot(StatementNode parseTreeRoot) {
		this.parseTreeRoot = parseTreeRoot;
	}

	public SchemaManager getSchemaManager() {
		return schemaManager;
	}

	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}

	public MainMemory getMemory() {
		return memory;
	}

	public void setMemory(MainMemory memory) {
		this.memory = memory;
	}

	public Disk getDisk() {
		return disk;
	}

	public void setDisk(Disk disk) {
		this.disk = disk;
	}

}
