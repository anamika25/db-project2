package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.MainMemory;
import storageManager.Relation;
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
					sorted = true;
				}
				if (hasDistinct) {
					if (!sorted) {
						// sort naturally
					}
					// remove duplicate
				}
			} else {
				// Two pass algorithm
			}
		} else {
			// Multiple tables
		}
	}

}
