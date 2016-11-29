package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import parser.StatementNode;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * To execute insert query
 */
public class InsertExecutor {

	public void execute(ExecutionParameter parameter) {
		SchemaManager schemaManager = parameter.getSchemaManager();
		MainMemory memory = parameter.getMemory();
		assert schemaManager != null;
		assert memory != null;

		List<StatementNode> insertParseTree = parameter.getParseTreeRoot().getBranches();
		String tableName = null;
		List<StatementNode> columns = null;
		for (StatementNode statementNode : insertParseTree) {
			String type = statementNode.getType();
			if (type.equalsIgnoreCase(Constants.TABLE))
				tableName = statementNode.getFirstChild().getType();
			else if (type.equalsIgnoreCase(Constants.COLUMNS)) {
				columns = statementNode.getBranches();
			} else if (type.equalsIgnoreCase(Constants.VALUES)) {
				Relation relation = schemaManager.getRelation(tableName);
				Tuple tuple = relation.createTuple();
				assert columns != null;
				int i = 0;
				for (StatementNode node : columns) {
					assert node.getType().equalsIgnoreCase(Constants.COLUMN_NAME);
					String fieldType = node.getFirstChild().getType();

					assert tuple.getSchema().getFieldType(fieldType) != null;
					assert statementNode.getBranches().get(i).getType().equalsIgnoreCase("VALUE");
					String value = statementNode.getBranches().get(i).getFirstChild().getType();

					if (tuple.getSchema().getFieldType(fieldType).equals(FieldType.INT)) {
						tuple.setField(fieldType, Integer.parseInt(value));
					} else {
						tuple.setField(fieldType, value);
					}
					i += 1;
				}
				appendTupleToRelation(relation, memory, 0, tuple);
			} else if (type.equalsIgnoreCase(Constants.SELECT)) {
				Relation tempRelation = HelperFunctions.selectHandler(schemaManager, memory, "SELECT * FROM course", 1);
				String[] tp = { "sid", "homework", "project", "exam", "grade" };
				ArrayList<String> tempList = new ArrayList<>(Arrays.asList(tp));
				HelperFunctions.insertFromSelect(schemaManager, memory, schemaManager.getRelation(tableName), tempList,
						tempRelation);
			}
		}
		System.out.println("Insert query executed");
	}

	private static void appendTupleToRelation(Relation table, MainMemory memory, int memoryBlockIndex, Tuple tuple) {
		Block blockReference;
		if (table.getNumOfBlocks() == 0) {
			blockReference = memory.getBlock(memoryBlockIndex);
			blockReference.clear();
			blockReference.appendTuple(tuple);
			table.setBlock(table.getNumOfBlocks(), memoryBlockIndex);
		} else {
			table.getBlock(table.getNumOfBlocks() - 1, memoryBlockIndex);
			blockReference = memory.getBlock(memoryBlockIndex);
			if (blockReference.isFull()) {
				blockReference.clear();
				blockReference.appendTuple(tuple);
				table.setBlock(table.getNumOfBlocks(), memoryBlockIndex);
			} else {
				blockReference.appendTuple(tuple);
				table.setBlock(table.getNumOfBlocks() - 1, memoryBlockIndex);
			}
		}
	}
}
