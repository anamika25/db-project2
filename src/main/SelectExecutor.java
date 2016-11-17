package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.Block;
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
		if (columnsNode.getBranches().get(0).getType().equals(Constants.DISTINCT)) {
			hasDistinct = true;
			columnsNode = columnsNode.getBranches().get(0);
		}

		List<String> selectColumnList = new ArrayList<String>();
		for (StatementNode columnNameNode : columnsNode.getBranches()) {
			if (!columnNameNode.getType().equals(Constants.COLUMN_NAME)) {
				System.out.println("Column name not found. Exiting!!!");
				System.exit(0);
			}
			selectColumnList.add(columnNameNode.getBranches().get(0).getType());
		}

		SchemaManager schemaManager = parameter.getSchemaManager();
		MainMemory memory = parameter.getMemory();

		// Select from single table
		if (fromNode.getBranches().size() == 1) {
			if (!fromNode.getBranches().get(0).equals(Constants.TABLE)) {
				System.out.println("Table node not found. Exiting!!!");
				System.exit(0);
			}
			String tableName = fromNode.getBranches().get(0).getBranches().get(0).getType();
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
					HelperFunctions.onePassSort(matchedTuples,
							orderByNode.getBranches().get(0).getBranches().get(0).getType());
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
				// TODO print header and tuples
			} else {
				// Two pass algorithm
				if (orderByNode == null && !hasDistinct) {
					System.out.println("Select operation without distinct and Order By");
					simpleSelectQuery(memory, table, selectColumnList, whereNode);
				} else {
					System.out.println("Select operation with order/distinct");
					String orderField = orderByNode == null ? null
							: orderByNode.getBranches().get(0).getBranches().get(0).getType();
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
		// TODO print header
		while (index < table.getNumOfBlocks()) {
			int blocksToRead = 0;
			if (table.getNumOfBlocks() - index > memory.getMemorySize())
				blocksToRead = memory.getMemorySize();
			else
				blocksToRead = table.getNumOfBlocks() - index;

			table.getBlocks(index, 0, blocksToRead);
			List<Tuple> tuples = memory.getTuples(0, blocksToRead);
			for (Tuple tuple : tuples) {
				if (whereNode == null || ExpressionEvaluator.evaluateLogicalOperator(whereNode, tuple)) {
					// TODO print tuple
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
			// TODO print header
			for (Tuple tuple : tuples) {
				// TODO print tuple
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

}
