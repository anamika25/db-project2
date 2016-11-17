package main;

import java.util.List;

import parser.StatementNode;

/**
 * To execute drop query
 */
public class DropExecutor {

	public void execute(ExecutionParameter parameter) {
		List<StatementNode> arguments = parameter.getParseTreeRoot().getBranches();
		if (arguments != null) {
			String tableName = arguments.get(0).getFirstChild().getType();
			parameter.getSchemaManager().deleteRelation(tableName);
			System.out.println("Successfully dropped table " + tableName);
		} else
			System.out.println("Arguments null for drop statement");
	}

}
