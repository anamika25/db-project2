package main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parser.StatementNode;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class ExecutionParameter {

	private Map<String, List<StatementNode>> valueMap = new HashMap<String, List<StatementNode>>();
	private SchemaManager schemaManager;
	private MainMemory memory;
	private Disk disk;

	public ExecutionParameter(HashMap<String, List<StatementNode>> map) {
		valueMap = map;
	}

	public ExecutionParameter(ExecutionParameter old) {
		schemaManager = old.getSchemaManager();
		memory = old.getMemory();
	}

	public Map<String, List<StatementNode>> getValueMap() {
		return valueMap;
	}

	public void setValueMap(Map<String, List<StatementNode>> map) {
		this.valueMap = map;
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
