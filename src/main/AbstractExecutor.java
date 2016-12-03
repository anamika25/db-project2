package main;

import java.util.ArrayList;
import java.util.List;

import parser.StatementNode;
import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * Class to call different query executors depending on query type
 */
public class AbstractExecutor {

	public void execute(StatementNode statement, SchemaManager schemaManager, Disk disk, MainMemory memory) {
		long startSystemTime = System.currentTimeMillis();
		double startDiskTime = disk.getDiskTimer();
		long startDiskIO = disk.getDiskIOs();

		ExecutionParameter parameter = new ExecutionParameter(statement, schemaManager, memory, disk);

		switch (statement.getType()) {
		case Constants.CREATE:
			new CreateExecutor().execute(parameter);
			break;
		case Constants.INSERT:
			new InsertExecutor().execute(parameter);
			break;
		case Constants.SELECT:
			List<Tuple> tuples = new SelectExecutor().execute(parameter);

			StatementNode columnsNode = null;
			for (StatementNode s : statement.getBranches()) {
				if (s.getType().equalsIgnoreCase(Constants.COLUMNS))
					columnsNode = s;
			}
			if (columnsNode.getFirstChild().getType().equalsIgnoreCase(Constants.DISTINCT)) {
				columnsNode = columnsNode.getFirstChild();
			}
			List<String> columns = new ArrayList<String>();
			List<String> headers = new ArrayList<String>();
			for (StatementNode field : columnsNode.getBranches()) {
				headers.add(field.getFirstChild().getType());
				if (field.getFirstChild().getType().contains("."))
					field.getFirstChild().setType(field.getFirstChild().getType().split("\\.")[1]);
				columns.add(field.getFirstChild().getType());
			}

			if (tuples != null && !tuples.isEmpty()) {
				printHeader(tuples.get(0), headers);
				for (Tuple tuple : tuples) {
					printTuple(tuple, columns);
				}
			}
			break;
		case Constants.DELETE:
			new DeleteExecutor().execute(parameter);
			break;
		case Constants.DROP:
			new DropExecutor().execute(parameter);
			break;
		}

		System.out.print("Computer elapse time = " + (System.currentTimeMillis() - startSystemTime) + " ms" + "\n");
		System.out.print("Calculated elapse time = " + (disk.getDiskTimer() - startDiskTime) + " ms" + "\n");
		System.out.println("Calculated Disk I/Os = " + (disk.getDiskIOs() - startDiskIO) + "\n");

	}

	private void printHeader(Tuple tuple, List<String> fieldList) {
		if (fieldList.get(0).equals("*")) {
			for (String fieldNames : tuple.getSchema().getFieldNames()) {
				System.out.print(fieldNames + "   ");
			}
			System.out.println();
		} else {
			for (String str : fieldList) {
				System.out.print(str + "    ");
			}
			System.out.println();
		}
	}

	private void printTuple(Tuple tuple, List<String> fieldList) {
		if (fieldList.get(0).equals("*")) {
			System.out.println(tuple);
			return;
		}
		for (String field : fieldList) {
			System.out.print((tuple.getSchema().getFieldType(field) == FieldType.INT ? tuple.getField(field).integer
					: tuple.getField(field).str) + "   ");
		}
		System.out.println();
	}

}
