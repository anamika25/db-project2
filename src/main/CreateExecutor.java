package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.FieldType;
import storageManager.Schema;

public class CreateExecutor extends AbstractExecutor {

	@Override
	protected void execute(ExecutionParameter parameter) {
		ArrayList<String> fieldName = new ArrayList<String>();
		ArrayList<FieldType> fieldType = new ArrayList<FieldType>();
		StatementNode table = parameter.getValueMap().get(Constants.CREATE).get(0);
		if (!table.getType().equalsIgnoreCase(Constants.RELATION)) {
			System.out.println("Table name not found for Create statement. Exiting!!");
			System.exit(0);
		}

		String tableName = table.getBranches().get(0).getType();
		List<StatementNode> columnsDetails = parameter.getValueMap().get(Constants.CREATE).get(1).getBranches();
		for (StatementNode node : columnsDetails) {
			if (!node.getType().equalsIgnoreCase(Constants.COLUMN_DETAILS)) {
				System.out.println("Column details not present for Create statement. Exiting!!");
				System.exit(0);
			}
			fieldName.add(node.getBranches().get(0).getBranches().get(0).getType());
			String type = node.getBranches().get(1).getBranches().get(0).getType();
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
