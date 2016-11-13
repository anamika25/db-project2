package main;

import java.util.List;

import parser.StatementNode;

public class DropExecutor extends AbstractExecutor {

	@Override
	protected void execute(ExecutionParameter parameter) {
		List<StatementNode> arguments = parameter.getValueMap().get(Constants.DROP);
		if (arguments != null) {
			String tableName = arguments.get(0).getBranches().get(0).getType();
			parameter.getSchemaManager().deleteRelation(tableName);
			System.out.println("Successfully dropped table " + tableName);
		} else
			System.out.println("Arguments null for drop statement");
	}

}
