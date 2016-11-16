package main;

import parser.StatementNode;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

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
			new SelectExecutor().execute(parameter);
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

}
