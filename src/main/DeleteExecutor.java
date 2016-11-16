package main;

import java.util.List;

import parser.StatementNode;
import storageManager.Block;
import storageManager.Relation;
import storageManager.Tuple;

/**
 * To execute delete query
 */
public class DeleteExecutor {

	public void execute(ExecutionParameter parameter) {
		List<StatementNode> deleteParseTree = parameter.getParseTreeRoot().getBranches();
		if (deleteParseTree == null) {
			System.out.println("Parse tree not found for DELETE statement. Exiting!!");
			System.exit(0);
		}

		StatementNode tableNode = null;
		StatementNode whereExpression = null;
		for (StatementNode argument : deleteParseTree) {
			if (argument.getType().equalsIgnoreCase(Constants.TABLE))
				tableNode = argument;
			if (argument.getType().equalsIgnoreCase(Constants.WHERE))
				whereExpression = argument;
		}

		if (tableNode == null) {
			System.out.println("Table name not found for DELETE statement. Exiting!!");
			System.exit(0);
		}

		Relation relationToDelete = parameter.getSchemaManager().getRelation(tableNode.getBranches().get(0).getType());
		if (whereExpression == null) {
			relationToDelete.deleteBlocks(0);
		} else {
			int blocks = relationToDelete.getNumOfBlocks();
			for (int i = 0; i < blocks; i++) {
				boolean modified = false;
				relationToDelete.getBlock(i, 0);
				Block firstMemoryBlock = parameter.getMemory().getBlock(0);
				List<Tuple> tuples = firstMemoryBlock.getTuples();
				for (int j = 0; j < tuples.size(); j++) {
					if (ExpressionEvaluator.evaluateLogicalOperator(whereExpression, tuples.get(j))) {
						firstMemoryBlock.invalidateTuple(j);
						modified = true;
					}
				}
				if (modified) {
					relationToDelete.setBlock(i, 0);
				}
			}
		}
	}

}
