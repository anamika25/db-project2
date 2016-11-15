package main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parser.StatementNode;

public class AbstractExecutor {

	protected static Map<String, AbstractExecutor> stringExecutorMap;
	static {
		stringExecutorMap.put(Constants.SELECT, new SelectExecutor());
		stringExecutorMap.put(Constants.CREATE, new CreateExecutor());
		stringExecutorMap.put(Constants.DROP, new DropExecutor());
		stringExecutorMap.put(Constants.INSERT, new InsertExecutor());
		stringExecutorMap.put(Constants.INITIAL, new AbstractExecutor());
		stringExecutorMap.put(Constants.DELETE, new DeleteExecutor());
	}

	protected void execute(ExecutionParameter parameter) {
		String state = parameter.getQueryTypeToParseTreeMap().get("INITIAL").get(0).getType();
		ExecutionParameter nextParameter = new ExecutionParameter(parameter);
		Map<String, List<StatementNode>> argu = new HashMap<String, List<StatementNode>>();
		List<StatementNode> list = parameter.getQueryTypeToParseTreeMap().get("INITIAL").get(0).getBranches();
		argu.put(state, list);
		nextParameter.setQueryTypeToParseTreeMap(argu);
		if (parameter.getSchemaManager() == null)
			System.out.print("No Schema manager found!!!");
		AbstractExecutor machine = stringExecutorMap.get(state);
		machine.execute(nextParameter);
	}

}
