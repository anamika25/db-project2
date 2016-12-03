package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	public List<Tuple> execute(ExecutionParameter parameter) {
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
		if (columnsNode.getFirstChild().getType().equals(Constants.DISTINCT)) {
			hasDistinct = true;
			columnsNode = columnsNode.getFirstChild();
		}

		List<String> selectColumnList = new ArrayList<String>();
		for (StatementNode columnNameNode : columnsNode.getBranches()) {
			if (!columnNameNode.getType().equals(Constants.COLUMN_NAME)) {
				System.out.println("Column name not found. Exiting!!!");
				System.exit(0);
			}
			selectColumnList.add(columnNameNode.getFirstChild().getType());
		}

		SchemaManager schemaManager = parameter.getSchemaManager();
		MainMemory memory = parameter.getMemory();

		// Select from single table
		if (fromNode.getBranches().size() == 1) {
			if (!fromNode.getFirstChild().getType().equals(Constants.TABLE)) {
				System.out.println("Table node not found. Exiting!!!");
				System.exit(0);
			}
			String tableName = fromNode.getFirstChild().getFirstChild().getType();
			Relation table = schemaManager.getRelation(tableName);

			// Condition for one pass algorithm
			if (table.getNumOfBlocks() <= memory.getMemorySize()) {
				table.getBlocks(0, 0, table.getNumOfBlocks());
				ArrayList<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
				ArrayList<Tuple> matchedTuples = new ArrayList<Tuple>();
				if (whereNode != null) {
					for (Tuple t : tuples) {
						if (ExpressionEvaluator.evaluateLogicalOperator(whereNode, t))
							matchedTuples.add(t);
					}
				} else
					matchedTuples = tuples;

				if (matchedTuples.isEmpty()) {
					System.out.println("No tuples found.");
					return null;
				}

				if (hasDistinct) {
					// sort by distinct columns
					// HelperFunctions.onePassSort(matchedTuples,
					// selectColumnList);
					// remove duplicate
					HelperFunctions.removeDuplicateTuplesOnePass(matchedTuples, selectColumnList);
				}
				if (orderByNode != null) {
					// Sort by given order
					HelperFunctions.onePassSort(matchedTuples,
							new ArrayList<>(Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
				}
				return matchedTuples;
			} else {
				// Two pass algorithm
				List<Tuple> outputTuples = null;
				if (orderByNode == null && !hasDistinct) {
					System.out.println("Select operation without distinct and Order By");
					outputTuples = simpleSelectQuery(memory, table, selectColumnList, whereNode);
				} else {
					System.out.println("Select operation with order/distinct");
					String orderField = orderByNode == null ? null
							: orderByNode.getFirstChild().getFirstChild().getType();
					outputTuples = complexSelectQuery(memory, schemaManager, table, selectColumnList, whereNode,
							orderField, hasDistinct);
				}
				return outputTuples;
			}
		} else {
			// TODO Multiple tables
			if (whereNode != null && whereNode.getFirstChild().getType().equals(Constants.EQUAL)) {
				StatementNode equalityNode = whereNode.getFirstChild();
				StatementNode firstOperand = equalityNode.getFirstChild();
				StatementNode secondOperand = equalityNode.getBranches().get(1);
				if (!firstOperand.getType().equals(Constants.COLUMN_NAME)
						|| !secondOperand.getType().equals(Constants.COLUMN_NAME)) {
					System.out.println("Wring parse tree for where condition. Exiting!!!");
					System.exit(0);
				}
				String table1 = firstOperand.getFirstChild().getType().split("\\.")[0];
				String column1 = firstOperand.getFirstChild().getType().split("\\.")[1];
				String table2 = secondOperand.getFirstChild().getType().split("\\.")[0];
				String column2 = secondOperand.getFirstChild().getType().split("\\.")[1];

				// if natural join can be applied
				if (column1.equals(column2)) {
					List<Tuple> naturalJoinTuples = HelperFunctions.naturalJoin(schemaManager, memory, table1, table2,
							column1);
					Relation naturalJoinTable = HelperFunctions.createTableFromTuples(schemaManager, memory,
							naturalJoinTuples, "natural_join_temp");

					if (!hasDistinct && orderByNode == null) {
						List<Tuple> tuples = HelperFunctions.filter(schemaManager, memory, naturalJoinTable, whereNode,
								selectColumnList);
						return tuples;
					}
					List<Tuple> newTuples = HelperFunctions.filter(schemaManager, memory, naturalJoinTable, whereNode,
							selectColumnList);
					Relation r1 = HelperFunctions.createTableFromTuples(schemaManager, memory, newTuples,
							"natural_join_temp_1");
					if (hasDistinct && orderByNode == null) {
						if (selectColumnList.get(0).equals("*")) {
							selectColumnList = r1.getSchema().getFieldNames();
						}
						List<Tuple> newTuples1 = null;
						if (r1.getNumOfBlocks() < memory.getMemorySize()) {
							// Relation temp =
							// HelperFunctions.createTableFromTuples(schemaManager,
							// memory,
							// HelperFunctions.onePassSortWrapper(schemaManager,
							// memory, r1, selectColumnList),
							// "temp");
							newTuples1 = HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, r1,
									selectColumnList);
						} else {
							// Relation temp =
							// HelperFunctions.createTableFromTuples(schemaManager,
							// memory,
							// HelperFunctions.twoPassSort(schemaManager,
							// memory, r1, selectColumnList), "temp");
							newTuples1 = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, r1,
									selectColumnList);
						}
						return newTuples1;
					}
					if (!hasDistinct && orderByNode != null) {
						List<Tuple> newTuples1 = null;
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							newTuples1 = HelperFunctions.onePassSortWrapper(schemaManager, memory, r1, new ArrayList<>(
									Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
						else
							newTuples1 = HelperFunctions.twoPassSort(schemaManager, memory, r1, new ArrayList<>(
									Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
						return newTuples1;
					}
					if (hasDistinct && orderByNode != null) {
						if (selectColumnList.get(0).equals("*")) {
							selectColumnList = r1.getSchema().getFieldNames();
						}
						List<Tuple> newTuples1 = null;
						if (r1.getNumOfBlocks() < memory.getMemorySize()) {
							// Relation temp =
							// HelperFunctions.createTableFromTuples(schemaManager,
							// memory,
							// HelperFunctions.onePassSortWrapper(schemaManager,
							// memory, r1, selectColumnList),
							// "temp");
							newTuples1 = HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, r1,
									selectColumnList);
						} else {
							// Relation temp =
							// HelperFunctions.createTableFromTuples(schemaManager,
							// memory,
							// HelperFunctions.twoPassSort(schemaManager,
							// memory, r1, selectColumnList), "temp");
							newTuples1 = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, r1,
									selectColumnList);
						}
						Relation tempRelation = HelperFunctions.createTableFromTuples(schemaManager, memory, newTuples1,
								r1.getRelationName() + "_distinct");

						List<Tuple> newTuples2 = null;
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							newTuples2 = HelperFunctions.onePassSortWrapper(schemaManager, memory, tempRelation,
									new ArrayList<>(
											Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
						else
							newTuples2 = HelperFunctions.twoPassSort(schemaManager, memory, tempRelation,
									new ArrayList<>(
											Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
						return newTuples2;
					}
					return null;
				}
			}

			// not natural join, cross join
			ArrayList<String> tableList = new ArrayList<>();
			for (StatementNode table : fromNode.getBranches()) {
				tableList.add(table.getFirstChild().getType());
			}

			if (!hasDistinct && orderByNode == null && columnsNode.getFirstChild().getFirstChild().getType().equals("*")
					&& whereNode == null) {
				Relation temp = multiRelationCrossJoin(schemaManager, memory, tableList);
				List<Tuple> tuples = new ArrayList<>();
				for (int i = 0; i < temp.getNumOfBlocks(); i++) {
					temp.getBlock(i, 0);
					Block block = memory.getBlock(0);
					for (Tuple t : block.getTuples()) {
						tuples.add(t);
					}
				}
				return tuples;
			}
			Relation relationAfterCross = multiRelationCrossJoin(schemaManager, memory, tableList);

			ArrayList<String> fields = new ArrayList<>();
			for (StatementNode nodes : columnsNode.getBranches()) {
				fields.add(nodes.getFirstChild().getType());
			}

			if (whereNode != null) {
				if (!hasDistinct && orderByNode == null) {
					return HelperFunctions.filter(schemaManager, memory, relationAfterCross, whereNode, fields);
				} else {
					relationAfterCross = HelperFunctions.createTableFromTuples(schemaManager, memory,
							HelperFunctions.filter(schemaManager, memory, relationAfterCross, whereNode, fields),
							"cross_join_output");
				}
			}

			if (hasDistinct) {
				if (fields.get(0).equals("*")) {
					fields = relationAfterCross.getSchema().getFieldNames();
				}
				if (orderByNode == null) {
					if (relationAfterCross.getNumOfBlocks() < memory.getMemorySize()) {
						// Relation temp =
						// HelperFunctions.createTableFromTuples(schemaManager,
						// memory,
						// HelperFunctions.onePassSortWrapper(schemaManager,
						// memory, relationAfterCross, fields),
						// "temp");
						return HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, relationAfterCross,
								fields);
					} else {
						// Relation temp =
						// HelperFunctions.createTableFromTuples(schemaManager,
						// memory,
						// HelperFunctions.twoPassSort(schemaManager, memory,
						// relationAfterCross, fields), "temp");
						return HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, relationAfterCross,
								fields);
					}
				} else {
					// Relation temp =
					// HelperFunctions.createTableFromTuples(schemaManager,
					// memory,
					// HelperFunctions.twoPassSort(schemaManager, memory,
					// relationAfterCross, fields), "temp");
					relationAfterCross = HelperFunctions.createTableFromTuples(schemaManager, memory,
							HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, relationAfterCross, fields),
							"cross_join_distinct");
				}
			}

			if (orderByNode != null) {
				return HelperFunctions.onePassSortWrapper(schemaManager, memory, relationAfterCross,
						new ArrayList<>(Arrays.asList(orderByNode.getFirstChild().getFirstChild().getType())));
			}

			if (whereNode == null && !fields.get(0).equals("*")) {
				int total = relationAfterCross.getNumOfBlocks();
				List<Tuple> output = new ArrayList<Tuple>();
				for (int i = 0; i < total; i++) {
					relationAfterCross.getBlock(i, 0);
					output.addAll(memory.getBlock(0).getTuples());
				}
				return output;
			}
		}
		return null;
	}

	private List<Tuple> simpleSelectQuery(MainMemory memory, Relation table, List<String> selectColumnList,
			StatementNode whereNode) {
		int index = 0;
		List<Tuple> output = new ArrayList<Tuple>();
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
					output.add(tuple);
				}
			}
			index += blocksToRead;
		}
		return output;
	}

	private List<Tuple> complexSelectQuery(MainMemory memory, SchemaManager schemaManager, Relation table,
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
		// table now contains only selected tuples

		if (table.getNumOfBlocks() <= memory.getMemorySize()) {
			// selected tuples can be processed in one-pass
			table.getBlocks(0, 0, table.getNumOfBlocks());
			ArrayList<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
			// HelperFunctions.onePassSort(tuples, selectColumnList);
			if (hasDistinct) {
				HelperFunctions.removeDuplicateTuplesOnePass(tuples, selectColumnList);
			}
			if (orderField != null)
				HelperFunctions.onePassSort(tuples, new ArrayList<>(Arrays.asList(orderField)));
			return tuples;
		} else {
			// two pass for sort and duplicate removal
			if (selectColumnList.get(0).equals("*")) {
				selectColumnList = table.getSchema().getFieldNames();
			}
			List<Tuple> outputTuples = null;
			if (hasDistinct && orderField != null) {
				// Relation temp =
				// HelperFunctions.createTableFromTuples(schemaManager, memory,
				// HelperFunctions.twoPassSort(schemaManager, memory, table,
				// selectColumnList), "temp");
				List<Tuple> tuples = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table,
						selectColumnList);
				outputTuples = HelperFunctions.twoPassSort(schemaManager, memory,
						HelperFunctions.createTableFromTuples(schemaManager, memory, tuples, "select_temp_sort"),
						new ArrayList<>(Arrays.asList(orderField)));
			} else if (hasDistinct) {
				// Relation temp =
				// HelperFunctions.createTableFromTuples(schemaManager, memory,
				// HelperFunctions.twoPassSort(schemaManager, memory, table,
				// selectColumnList), "temp");
				outputTuples = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table, selectColumnList);
			} else if (orderField != null) {
				outputTuples = HelperFunctions.twoPassSort(schemaManager, memory, table,
						new ArrayList<>(Arrays.asList(orderField)));
			}
			return outputTuples;
		}

	}

	public static Relation multiRelationCrossJoin(SchemaManager schemaManager, MainMemory memory,
			ArrayList<String> tableNames) {
		if (tableNames.size() == 2) {
			return HelperFunctions.executeCrossJoin(schemaManager, memory, tableNames);
		} else {
			// DP algorithm to determine join order
			Map<Set<String>, CrossJoinTablesHelper> singleRelation = new HashMap<Set<String>, CrossJoinTablesHelper>();
			for (String tableName : tableNames) {
				HashSet<String> set = new HashSet<String>();
				set.add(tableName);
				Relation table = schemaManager.getRelation(tableName);
				CrossJoinTablesHelper temp = new CrossJoinTablesHelper(set, table.getNumOfBlocks(),
						table.getNumOfTuples());
				temp.setCost(table.getNumOfBlocks());
				temp.setNumFields(table.getSchema().getNumOfFields());
				singleRelation.put(set, temp);
			}
			List<Map<Set<String>, CrossJoinTablesHelper>> costRelationList = new ArrayList<Map<Set<String>, CrossJoinTablesHelper>>();
			costRelationList.add(singleRelation);
			for (int i = 1; i < tableNames.size(); i++) {
				costRelationList.add(new HashMap<Set<String>, CrossJoinTablesHelper>());
			}

			Set<String> finalGoal = new HashSet<String>(tableNames);
			CrossJoinTablesHelper optimalJoinTables = HelperFunctions.findOptimalJoinOrder(costRelationList, finalGoal,
					memory.getMemorySize());
			HelperFunctions.travesal(optimalJoinTables, 0);
			return helper(optimalJoinTables, memory, schemaManager);

			// TODO
		}
	}

	public static Relation helper(CrossJoinTablesHelper optimalJoinTables, MainMemory mem,
			SchemaManager schemaManager) {
		if (optimalJoinTables.getJoinBy() == null || optimalJoinTables.getJoinBy().size() < 2) {
			List<String> relation = new ArrayList<>(optimalJoinTables.getTables());
			assert relation.size() == 1;
			return schemaManager.getRelation(relation.get(0));
		} else {
			assert optimalJoinTables.getJoinBy().size() == 2;
			String subRelation1 = helper(optimalJoinTables.getJoinBy().get(0), mem, schemaManager).getRelationName();
			String subRelation2 = helper(optimalJoinTables.getJoinBy().get(1), mem, schemaManager).getRelationName();
			ArrayList<String> relationName = new ArrayList<String>();
			relationName.add(subRelation1);
			relationName.add(subRelation2);
			return HelperFunctions.executeCrossJoin(schemaManager, mem, relationName);
		}
	}

}
