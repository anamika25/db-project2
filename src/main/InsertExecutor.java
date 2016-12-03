package main;

import java.util.ArrayList;
import java.util.List;

import parser.ParserException;
import parser.StatementNode;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * To execute insert query
 */
public class InsertExecutor {

	public void execute(ExecutionParameter parameter) throws ParserException {
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
				ExecutionParameter selectParameter = new ExecutionParameter(statementNode, schemaManager, memory,
						parameter.getDisk());
				List<Tuple> tuples = new SelectExecutor().execute(selectParameter);
				insertFromSelect(schemaManager, memory, schemaManager.getRelation(tableName), tuples);
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

	private static void insertFromSelect(SchemaManager schemaManager, MainMemory memory, Relation insertTable,
			List<Tuple> tuples) {
		ArrayList<FieldType> fieldTypes = new ArrayList<>();
		Schema selectTuplesSchema = tuples.get(0).getSchema();
		ArrayList<String> fields = selectTuplesSchema.getFieldNames();
		if (fields.size() == 1 && fields.get(0).equals("*")) {
			fields.remove(0);
			for (int i = 0; i < selectTuplesSchema.getNumOfFields(); i++) {
				fields.add(selectTuplesSchema.getFieldName(i));
			}
		}

		for (String s : fields) {
			fieldTypes.add(selectTuplesSchema.getFieldType(s));
		}
		Schema schema = new Schema(fields, fieldTypes);
		if (schemaManager.relationExists("from_select"))
			schemaManager.deleteRelation("from_select");

		Relation relation = schemaManager.createRelation("from_select", schema);
		ArrayList<Tuple> output = new ArrayList<>();

		for (Tuple t : tuples) {
			Tuple tuple = relation.createTuple();
			for (int j = 0; j < fields.size(); j++) {
				if (t.getField(fields.get(j)).type == FieldType.INT)
					tuple.setField(j, Integer.parseInt(t.getField(fields.get(j)).toString()));
				else
					tuple.setField(j, t.getField(fields.get(j)).toString());
			}
			output.add(tuple);
		}

		int count = insertTable.getNumOfBlocks();
		Block block = memory.getBlock(0);
		while (!output.isEmpty()) {
			block.clear();
			for (int i = 0; i < schema.getTuplesPerBlock(); i++) {
				if (!output.isEmpty()) {
					Tuple t = output.get(0);
					block.setTuple(i, t);
					output.remove(t);
				}
			}
			insertTable.setBlock(count++, 0);
		}
	}
}
