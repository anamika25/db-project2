package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.FieldType;
import storageManager.Schema;

/**
 * To execute create query
 */
public class CreateExecutor {

	public void execute(ExecutionParameter parameter) {
		ArrayList<String> fieldName = new ArrayList<String>();
		ArrayList<FieldType> fieldType = new ArrayList<FieldType>();
		List<StatementNode> createParseTree = parameter.getParseTreeRoot().getBranches();
		StatementNode tableNode = createParseTree.get(0);
		if (!tableNode.getType().equalsIgnoreCase(Constants.TABLE)) {
			System.out.println("Table name not found for Create statement. Exiting!!");
			System.exit(0);
		}

		String tableName = tableNode.getFirstChild().getType();
		List<StatementNode> columnsDetails = createParseTree.get(1).getBranches();
		for (StatementNode columnNode : columnsDetails) {
			if (!columnNode.getType().equalsIgnoreCase(Constants.COLUMN_DETAILS)) {
				System.out.println("Column details not present for Create statement. Exiting!!");
				System.exit(0);
			}
			fieldName.add(columnNode.getFirstChild().getFirstChild().getType());
			String type = columnNode.getBranches().get(1).getFirstChild().getType();
			if (type.equals(FieldType.INT.name())) {
				fieldType.add(FieldType.INT);
			} else if (type.equals(FieldType.STR20.name())) {
				fieldType.add(FieldType.STR20);
			} else {
				System.out.println("Wrong datatype for Create statement. Exiting!!");
				System.exit(0);
			}
		}
		Schema schema = new Schema(fieldName, fieldType);
		parameter.getSchemaManager().createRelation(tableName, schema);
		System.out.println("Successfully created table " + tableName);
	}

}
