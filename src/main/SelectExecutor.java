package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * To execute select query
 */
public class SelectExecutor {

	public void execute(ExecutionParameter parameter) {
		StatementNode columnsNode = null, fromNode = null, orderByNode = null, whereNode = null;

		// find different parts of select statement
		for (StatementNode node : parameter.getParseTreeRoot().getBranches()) {
			if (node.getType().equals(Constants.COLUMNS))
				columnsNode = node;
			else if (node.getType().equals(Constants.FROM))
				fromNode = node;
			else if (node.getType().equals(Constants.WHERE))
				whereNode = node;
			else if (node.getType().equals(Constants.ORDER))
				orderByNode = node;
		}
		// check if statement has select-list and table-name
		assert columnsNode != null;
		assert fromNode != null;

		boolean hasDistinct = false;
		if (columnsNode.getFirstChild().getType().equals(Constants.DISTINCT)) {
			hasDistinct = true;
			columnsNode = columnsNode.getFirstChild();
		}

		List<String> selectColumnList = new ArrayList<String>();
		for (StatementNode columnNameNode : columnsNode.getBranches()) {
			if (!columnNameNode.getType().equals(Constants.COLUMN_NAME)) {
				System.out.println("Column name not found. Exiting!!!");
				System.exit(0);
			}
			selectColumnList.add(columnNameNode.getFirstChild().getType());
		}

		SchemaManager schemaManager = parameter.getSchemaManager();
		MainMemory memory = parameter.getMemory();

		// Select from single table
		if (fromNode.getBranches().size() == 1) {
			if (!fromNode.getFirstChild().equals(Constants.TABLE)) {
				System.out.println("Table node not found. Exiting!!!");
				System.exit(0);
			}
			String tableName = fromNode.getFirstChild().getFirstChild().getType();
			Relation table = schemaManager.getRelation(tableName);

			// Condition for one pass algorithm
			if (table.getNumOfBlocks() <= memory.getMemorySize()) {
				table.getBlocks(0, 0, table.getNumOfBlocks());
				List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
				List<Tuple> matchedTuples = new ArrayList<Tuple>();
				if (whereNode != null) {
					for (Tuple t : tuples) {
						if (ExpressionEvaluator.evaluateLogicalOperator(whereNode, t))
							matchedTuples.add(t);
					}
				}
				if (matchedTuples.isEmpty()) {
					System.out.println("No tuples found.");
					return;
				}

				boolean sorted = false;
				if (orderByNode != null) {
					// Sort by given order
					HelperFunctions.onePassSort(matchedTuples, orderByNode.getFirstChild().getFirstChild().getType());
					sorted = true;
				}
				if (hasDistinct) {
					if (!sorted) {
						// sort naturally
						HelperFunctions.onePassSort(matchedTuples, null);
					}
					// remove duplicate
					HelperFunctions.removeDuplicateTuplesOnePass(matchedTuples, selectColumnList);
				}
				// print header and tuples
				printHeader(matchedTuples.get(0), selectColumnList);
				for (Tuple tuple : matchedTuples)
					printTuple(tuple, selectColumnList);
			} else {
				// Two pass algorithm
				if (orderByNode == null && !hasDistinct) {
					System.out.println("Select operation without distinct and Order By");
					simpleSelectQuery(memory, table, selectColumnList, whereNode);
				} else {
					System.out.println("Select operation with order/distinct");
					String orderField = orderByNode == null ? null
							: orderByNode.getFirstChild().getFirstChild().getType();
					complexSelectQuery(memory, schemaManager, table, selectColumnList, whereNode, orderField,
							hasDistinct);
				}
			}
		} else {
			// Multiple tables
		}
	}

	private void simpleSelectQuery(MainMemory memory, Relation table, List<String> selectColumnList,
			StatementNode whereNode) {
		int index = 0;
		boolean headerPrinted = false;
		while (index < table.getNumOfBlocks()) {
			int blocksToRead = 0;
			if (table.getNumOfBlocks() - index > memory.getMemorySize())
				blocksToRead = memory.getMemorySize();
			else
				blocksToRead = table.getNumOfBlocks() - index;

			table.getBlocks(index, 0, blocksToRead);
			List<Tuple> tuples = memory.getTuples(0, blocksToRead);
			if (!headerPrinted) {
				// print header
				printHeader(tuples.get(0), selectColumnList);
				headerPrinted = true;
			}
			for (Tuple tuple : tuples) {
				if (whereNode == null || ExpressionEvaluator.evaluateLogicalOperator(whereNode, tuple)) {
					// print tuple
					printTuple(tuple, selectColumnList);
				}
			}
			index += blocksToRead;
		}
	}

	private void complexSelectQuery(MainMemory memory, SchemaManager schemaManager, Relation table,
			List<String> selectColumnList, StatementNode whereNode, String orderField, boolean hasDistinct) {
		// if where clause is given
		if (whereNode != null) {
			Schema schema = table.getSchema();
			Relation tempTable = schemaManager.createRelation(table.getRelationName() + "_temp", schema);
			Block tempBlock = memory.getBlock(1);
			tempBlock.clear();
			int tempTableIndex = 0;
			for (int i = 0; i < table.getNumOfBlocks(); i++) {
				table.getBlock(i, 0);
				ArrayList<Tuple> tuples = memory.getBlock(0).getTuples();
				for (Tuple tuple : tuples) {
					if (ExpressionEvaluator.evaluateLogicalOperator(whereNode, tuple)) {
						if (!tempBlock.isFull())
							tempBlock.appendTuple(tuple);
						else {
							memory.setBlock(1, tempBlock);
							tempBlock.clear();
							tempBlock.appendTuple(tuple);
							tempTable.setBlock(tempTableIndex, 1);
							tempTableIndex += 1;
						}
					}
				}
			}

			if (!tempBlock.isEmpty()) {
				memory.setBlock(1, tempBlock);
				tempTable.setBlock(tempTableIndex, 1);
				tempBlock.clear();
			}
			table = tempTable;
		}
		// table not contains only selected tuples

		if (table.getNumOfBlocks() <= memory.getMemorySize()) {
			// selected tuples can be processed in one-pass
			table.getBlocks(0, 0, table.getNumOfBlocks());
			ArrayList<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
			HelperFunctions.onePassSort(tuples, orderField);
			if (hasDistinct) {
				HelperFunctions.removeDuplicateTuplesOnePass(tuples, selectColumnList);
			}
			// print header
			printHeader(tuples.get(0), selectColumnList);
			for (Tuple tuple : tuples) {
				// print tuple
				printTuple(tuple, selectColumnList);
			}
		} else {
			// two pass for sort and duplicate removal
			if (selectColumnList.get(0).equals("*")) {
				selectColumnList = table.getSchema().getFieldNames();
			}
			if (hasDistinct && orderField != null) {
				table = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table, selectColumnList, true);
				HelperFunctions.twoPassSort(schemaManager, memory, table, orderField, false);
			} else if (hasDistinct) {
				HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table, selectColumnList, false);
			} else if (orderField != null) {
				HelperFunctions.twoPassSort(schemaManager, memory, table, orderField, false);
			}
		}

	}

	private void printHeader(Tuple tuple, List<String> columnList) {
		if (columnList.get(0).equals("*")) {
			for (String fieldNames : tuple.getSchema().getFieldNames()) {
				System.out.print(fieldNames + "   ");
			}
			System.out.println();
		} else {
			for (String str : columnList) {
				System.out.print(str + "    ");
			}
			System.out.println();
		}
	}

	private void printTuple(Tuple tuple, List<String> columnList) {
		if (columnList.get(0).equals("*")) {
			System.out.println(tuple);
			return;
		}
		for (String field : columnList) {
			System.out.print((tuple.getSchema().getFieldType(field) == FieldType.INT ? tuple.getField(field).integer
					: tuple.getField(field).str) + "   ");
		}
		System.out.println();
	}

}
