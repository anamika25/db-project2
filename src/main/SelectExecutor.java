package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import parser.StatementNode;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
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
			if (!fromNode.getFirstChild().equals(Constants.TABLE)) {
				System.out.println("Table node not found. Exiting!!!");
				System.exit(0);
			}
			String tableName = fromNode.getFirstChild().getFirstChild().getType();
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
					HelperFunctions.onePassSort(matchedTuples, orderByNode.getFirstChild().getFirstChild().getType());
					sorted = true;
				}
				if (hasDistinct) {
					if (!sorted) {
						// sort naturally
						HelperFunctions.onePassSort(matchedTuples, null);
					}
					// remove duplicate
					HelperFunctions.removeDuplicateTuplesOnePass(matchedTuples, selectColumnList);
				}
				// print header and tuples
				printHeader(matchedTuples.get(0), selectColumnList);
				for (Tuple tuple : matchedTuples)
					printTuple(tuple, selectColumnList);
			} else {
				// Two pass algorithm
				if (orderByNode == null && !hasDistinct) {
					System.out.println("Select operation without distinct and Order By");
					simpleSelectQuery(memory, table, selectColumnList, whereNode);
				} else {
					System.out.println("Select operation with order/distinct");
					String orderField = orderByNode == null ? null
							: orderByNode.getFirstChild().getFirstChild().getType();
					complexSelectQuery(memory, schemaManager, table, selectColumnList, whereNode, orderField,
							hasDistinct);
				}
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
				String table1 = firstOperand.getFirstChild().getType().split(".")[0];
				String column1 = firstOperand.getFirstChild().getType().split(".")[1];
				String table2 = secondOperand.getFirstChild().getType().split(".")[0];
				String column2 = secondOperand.getFirstChild().getType().split(".")[1];

				// if natural join can be applied
				if (column1.equals(column2)) {
					Relation r = HelperFunctions.naturalJoin(schemaManager, memory, table1, table2, column1, true);

					if (!hasDistinct && orderByNode == null) {
						HelperFunctions.filter(schemaManager, memory, r, whereNode, selectColumnList, false);
						return;
					}
					Relation r1 = HelperFunctions.filter(schemaManager, memory, r, whereNode, selectColumnList, true);
					if (hasDistinct && orderByNode == null) {
						if (selectColumnList.get(0).equals("*")) {
							selectColumnList = r1.getSchema().getFieldNames();
						}
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, r1, selectColumnList,
									false);
						else
							HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, r1, selectColumnList, false);
						return;
					}
					if (!hasDistinct && orderByNode != null) {
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							HelperFunctions.onePassSortWrapper(schemaManager, memory, r1,
									orderByNode.getFirstChild().getFirstChild().getType(), false);
						else
							HelperFunctions.twoPassSort(schemaManager, memory, r1,
									orderByNode.getFirstChild().getFirstChild().getType(), false);
						return;
					}
					if (hasDistinct && orderByNode != null) {
						if (selectColumnList.get(0).equals("*")) {
							selectColumnList = r1.getSchema().getFieldNames();
						}
						Relation tempRelation;
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							tempRelation = HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, r1,
									selectColumnList, true);
						else
							tempRelation = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, r1,
									selectColumnList, true);
						if (r1.getNumOfBlocks() < memory.getMemorySize())
							HelperFunctions.onePassSortWrapper(schemaManager, memory, tempRelation,
									orderByNode.getFirstChild().getFirstChild().getType(), false);
						else
							HelperFunctions.twoPassSort(schemaManager, memory, tempRelation,
									orderByNode.getFirstChild().getFirstChild().getType(), false);
						return;
					}
					return;
				}
			}

			// not natural join, cross join
			ArrayList<String> relationList = new ArrayList<>();
			for (StatementNode table : fromNode.getBranches()) {
				assert table.getType().equalsIgnoreCase(Constants.TABLE);
				relationList.add(table.getFirstChild().getType());
			}

			if (!hasDistinct && orderByNode == null && columnsNode.getFirstChild().getFirstChild().getType().equals("*")
					&& whereNode == null) {
				multiRelationCrossJoin(schemaManager, memory, relationList, false);
				return;
			}
			Relation relationAfterCross = multiRelationCrossJoin(schemaManager, memory, relationList, true);

			ArrayList<String> fields = new ArrayList<>();
			for (StatementNode nodes : columnsNode.getBranches()) {
				fields.add(nodes.getFirstChild().getType());
			}

			if (whereNode != null) {
				if (!hasDistinct && orderByNode == null) {
					HelperFunctions.filter(schemaManager, memory, relationAfterCross, whereNode, fields, false);
					return;
				} else {
					relationAfterCross = HelperFunctions.filter(schemaManager, memory, relationAfterCross, whereNode,
							fields, true);
				}
			}

			if (hasDistinct) {
				if (fields.get(0).equals("*")) {
					fields = relationAfterCross.getSchema().getFieldNames();
				}
				if (orderByNode == null) {
					if (relationAfterCross.getNumOfBlocks() < memory.getMemorySize())
						HelperFunctions.removeDuplicatesOnePassWrapper(schemaManager, memory, relationAfterCross,
								fields, false);
					else
						return;
				} else {
					relationAfterCross = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory,
							relationAfterCross, fields, true);
				}
			}

			if (orderByNode != null) {
				HelperFunctions.onePassSortWrapper(schemaManager, memory, relationAfterCross,
						orderByNode.getFirstChild().getFirstChild().getType(), false);
				return;
			}

			if (whereNode == null && !fields.get(0).equals("*")) {
				int total = relationAfterCross.getNumOfBlocks();
				for (int i = 0; i < total; i++) {
					relationAfterCross.getBlock(i, 0);
					ArrayList<Tuple> tuples = memory.getBlock(0).getTuples();
					for (Tuple tp : tuples) {
						for (String f : fields) {
							System.out.print(tp.getField(f).toString() + "  ");
						}
						System.out.println();
					}
				}
			}
		}
		return;
	}

	private void simpleSelectQuery(MainMemory memory, Relation table, List<String> selectColumnList,
			StatementNode whereNode) {
		int index = 0;
		boolean headerPrinted = false;
		while (index < table.getNumOfBlocks()) {
			int blocksToRead = 0;
			if (table.getNumOfBlocks() - index > memory.getMemorySize())
				blocksToRead = memory.getMemorySize();
			else
				blocksToRead = table.getNumOfBlocks() - index;

			table.getBlocks(index, 0, blocksToRead);
			List<Tuple> tuples = memory.getTuples(0, blocksToRead);
			if (!headerPrinted) {
				// print header
				printHeader(tuples.get(0), selectColumnList);
				headerPrinted = true;
			}
			for (Tuple tuple : tuples) {
				if (whereNode == null || ExpressionEvaluator.evaluateLogicalOperator(whereNode, tuple)) {
					// print tuple
					printTuple(tuple, selectColumnList);
				}
			}
			index += blocksToRead;
		}
	}

	private void complexSelectQuery(MainMemory memory, SchemaManager schemaManager, Relation table,
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
			HelperFunctions.onePassSort(tuples, orderField);
			if (hasDistinct) {
				HelperFunctions.removeDuplicateTuplesOnePass(tuples, selectColumnList);
			}
			// print header
			printHeader(tuples.get(0), selectColumnList);
			for (Tuple tuple : tuples) {
				// print tuple
				printTuple(tuple, selectColumnList);
			}
		} else {
			// two pass for sort and duplicate removal
			if (selectColumnList.get(0).equals("*")) {
				selectColumnList = table.getSchema().getFieldNames();
			}
			if (hasDistinct && orderField != null) {
				table = HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table, selectColumnList, true);
				HelperFunctions.twoPassSort(schemaManager, memory, table, orderField, false);
			} else if (hasDistinct) {
				HelperFunctions.removeDuplicatesTwoPass(schemaManager, memory, table, selectColumnList, false);
			} else if (orderField != null) {
				HelperFunctions.twoPassSort(schemaManager, memory, table, orderField, false);
			}
		}

	}

	public static Relation multiRelationCrossJoin(SchemaManager schemaManager, MainMemory memory,
			ArrayList<String> relationName, boolean returnTable) {
		int memsize = memory.getMemorySize();
		if (relationName.size() == 2) {
			return HelperFunctions.executeCrossJoin(schemaManager, memory, relationName, returnTable);
		} else {
			// DP algorithm to determine join order
			HashMap<Set<String>, CrossRelation> singleRelation = new HashMap<>();
			for (String name : relationName) {
				HashSet<String> set = new HashSet<>();
				set.add(name);
				Relation relation = schemaManager.getRelation(name);
				CrossRelation temp = new CrossRelation(set, relation.getNumOfBlocks(), relation.getNumOfTuples());
				temp.cost = relation.getNumOfBlocks();
				temp.fieldNum = relation.getSchema().getNumOfFields();
				singleRelation.put(set, temp);
			}
			List<HashMap<Set<String>, CrossRelation>> costRelationList = new ArrayList<>();
			costRelationList.add(singleRelation);
			for (int i = 1; i < relationName.size(); i++) {
				costRelationList.add(new HashMap<Set<String>, CrossRelation>());
			}

			Set<String> finalGoal = new HashSet<>(relationName);
			CrossRelation cr = Algorithms.findOptimal(costRelationList, finalGoal, memsize);
			Algorithms.travesal(cr, 0);
			if (mode == 0) {
				helper(cr, memory, schemaManager, 0);
			} else {
				return helper(cr, memory, schemaManager, 1);
			}

			// TODO
			return null;
		}
	}

	public static Relation helper(CrossRelation cr, MainMemory mem, SchemaManager schemaManager, int mode) {
		// mode 0 display, mode 1 output
		if (cr.joinBy == null || cr.joinBy.size() < 2) {
			List<String> relation = new ArrayList<>(cr.subRelation);
			assert relation.size() == 1;
			return schemaManager.getRelation(relation.get(0));
		} else {
			assert cr.joinBy.size() == 2;
			if (mode == 0) {
				String subRelation1 = helper(cr.joinBy.get(0), mem, schemaManager, 1).getRelationName();
				String subRelation2 = helper(cr.joinBy.get(1), mem, schemaManager, 1).getRelationName();
				ArrayList<String> relationName = new ArrayList<>();
				relationName.add(subRelation1);
				relationName.add(subRelation2);
				return Api.executeCrossJoin(schemaManager, mem, relationName, 0);
			} else {
				String subRelation1 = helper(cr.joinBy.get(0), mem, schemaManager, 1).getRelationName();
				String subRelation2 = helper(cr.joinBy.get(1), mem, schemaManager, 1).getRelationName();
				ArrayList<String> relationName = new ArrayList<>();
				relationName.add(subRelation1);
				relationName.add(subRelation2);
				return Api.executeCrossJoin(schemaManager, mem, relationName, 1);
			}
		}
	}

	private void printHeader(Tuple tuple, List<String> columnList) {
		if (columnList.get(0).equals("*")) {
			for (String fieldNames : tuple.getSchema().getFieldNames()) {
				System.out.print(fieldNames + "   ");
			}
			System.out.println();
		} else {
			for (String str : columnList) {
				System.out.print(str + "    ");
			}
			System.out.println();
		}
	}

	private void printTuple(Tuple tuple, List<String> columnList) {
		if (columnList.get(0).equals("*")) {
			System.out.println(tuple);
			return;
		}
		for (String field : columnList) {
			System.out.print((tuple.getSchema().getFieldType(field) == FieldType.INT ? tuple.getField(field).integer
					: tuple.getField(field).str) + "   ");
		}
		System.out.println();
	}

}
