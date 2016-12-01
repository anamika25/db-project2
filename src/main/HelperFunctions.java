package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parser.StatementNode;
import storageManager.Block;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * Class having helper functions like sorting, duplicate removal, etc.
 */
public class HelperFunctions {

	/**
	 * Sort tuples in place ordered by given column
	 */
	public static void onePassSort(List<Tuple> tuples, String orderByColumn) {
		List<String> columnNames = tuples.get(0).getSchema().getFieldNames();
		Collections.sort(tuples, new Comparator<Tuple>() {

			@Override
			public int compare(Tuple t1, Tuple t2) {
				return compareTuple(t1, t2, columnNames, orderByColumn);
			}
		});
	}

	public static Relation executeCrossJoin(SchemaManager schemaManager, MainMemory memory,
			ArrayList<String> tableNames) {

		if (!tableNames.isEmpty() && tableNames.size() == 2) {
			String table1 = tableNames.get(0);
			String table2 = tableNames.get(1);
			System.out.println("Applying cross join");
			Relation r = schemaManager.getRelation(table1);
			Relation s = schemaManager.getRelation(table2);

			Relation smallerTable = (r.getNumOfBlocks() <= s.getNumOfBlocks()) ? r : s;

			ArrayList<Tuple> output = new ArrayList<Tuple>();
			if (smallerTable.getNumOfBlocks() < memory.getMemorySize() - 1) {
				output = onePassJoin(schemaManager, memory, r, s);
			} else {
				output = loopJoin(schemaManager, memory, r, s);
			}
			Schema schema = combineSchema(schemaManager, r, s);
			String crossTableName = table1 + "_cross_" + table2;
			if (schemaManager.relationExists(crossTableName)) {
				schemaManager.deleteRelation(crossTableName);
			}
			Relation table = schemaManager.createRelation(crossTableName, schema);

			int count = 0;
			Block block = memory.getBlock(0);
			while (!output.isEmpty()) {
				block.clear();
				for (int i = 0; i < schema.getTuplesPerBlock(); i++) {
					if (!output.isEmpty()) {
						Tuple t = output.get(0);
						block.setTuple(i, t);
						output.remove(t);
					}
				}
				table.setBlock(count++, 0);
			}
			return table;
		} else {
			return null;
		}
	}

	public static ArrayList<Tuple> onePassJoin(SchemaManager schemaManager, MainMemory memory, Relation table1,
			Relation table2) {

		Relation smaller = (table1.getNumOfBlocks() <= table2.getNumOfBlocks()) ? table1 : table2;
		Relation larger = (smaller == table1) ? table2 : table1;
		smaller.getBlocks(0, 0, smaller.getNumOfBlocks());

		Schema schema = combineSchema(schemaManager, table1, table2);

		String tempTableName = table1 + "_cross_" + table2 + "_tmp";
		if (schemaManager.relationExists(tempTableName))
			schemaManager.deleteRelation(tempTableName);
		Relation table = schemaManager.createRelation(tempTableName, schema);

		ArrayList<Tuple> output = new ArrayList<Tuple>();
		for (int j = 0; j < larger.getNumOfBlocks(); j++) {
			larger.getBlock(j, memory.getMemorySize() - 1);
			Block largerblock = memory.getBlock(memory.getMemorySize() - 1);
			for (Tuple tuple1 : memory.getTuples(0, smaller.getNumOfBlocks())) {
				for (Tuple tuple2 : largerblock.getTuples()) {
					if (smaller == table1) {
						output.add(mergeTuple(schemaManager, table, tuple1, tuple2));
					} else {
						output.add(mergeTuple(schemaManager, table, tuple2, tuple1));
					}
				}
			}
		}
		return output;
	}

	public static ArrayList<Tuple> loopJoin(SchemaManager schemaManager, MainMemory memory, Relation table1,
			Relation table2) {
		Schema schema = combineSchema(schemaManager, table1, table2);
		String tempTableName = table1 + "cross" + table2 + "tmp";
		if (schemaManager.relationExists(tempTableName))
			schemaManager.deleteRelation(tempTableName);
		Relation table = schemaManager.createRelation(tempTableName, schema);

		ArrayList<Tuple> output = new ArrayList<Tuple>();
		for (int i = 0; i < table1.getNumOfBlocks(); i++) {
			table1.getBlock(i, 0);
			Block t1block = memory.getBlock(0);
			for (int j = 0; j < table2.getNumOfBlocks(); j++) {
				table2.getBlock(j, 1);
				Block t2block = memory.getBlock(1);
				for (Tuple tuple1 : t1block.getTuples()) {
					for (Tuple tuple2 : t2block.getTuples()) {
						output.add(mergeTuple(schemaManager, table, tuple1, tuple2));
					}
				}
			}
		}
		return output;
	}

	private static int compareTuple(Tuple t1, Tuple t2, List<String> columnNames, String orderByColumn) {
		// if order by given
		if (orderByColumn != null) {
			int val = compareTupleAtColumn(t1, t2, orderByColumn);
			// if not equal then return
			if (val != 0)
				return val;
		}
		// sort by other columns
		for (String column : columnNames) {
			if (!column.equalsIgnoreCase(orderByColumn)) {
				int val = compareTupleAtColumn(t1, t2, column);
				// if not equal then return, else compare other columns
				if (val != 0)
					return val;
			}
		}

		return 0;
	}

	private static int compareTupleAtColumn(Tuple t1, Tuple t2, String column) {
		Field tuple1attribute = t1.getField(column);
		Field tuple2attribute = t2.getField(column);
		if (tuple1attribute.type == FieldType.INT) {
			return Integer.compare(tuple1attribute.integer, tuple2attribute.integer);
		} else {
			return tuple1attribute.str.compareToIgnoreCase(tuple2attribute.str);
		}
	}

	/**
	 * Function to remove duplicate tuples for given columns assuming tuples are
	 * already sorted
	 */
	public static void removeDuplicateTuplesOnePass(List<Tuple> tuples, List<String> columns) {
		Tuple tuple = tuples.get(0);
		if (columns.get(0).equals("*")) {
			columns = tuple.getSchema().getFieldNames();
		}

		int nonDupIndex = 1, index = 1;
		while (index < tuples.size()) {
			if (compareTuple(tuple, tuples.get(index), columns, null) != 0) {
				tuple = tuples.get(index);
				tuples.set(nonDupIndex, tuples.get(index));
				nonDupIndex += 1;
			}
			index += 1;
		}
		for (int i = tuples.size() - 1; i >= nonDupIndex; i--) {
			tuples.remove(i);
		}
	}

	public static List<Tuple> twoPassSort(SchemaManager schemaManager, MainMemory memory, Relation table,
			String orderField) {

		// Phase 1
		int lastBlockIndex = twoPassPhaseOne(table, memory, orderField);

		// Phase 2
		int temp = 0;
		ArrayList<Integer> subListIndexes = new ArrayList<Integer>();
		ArrayList<Tuple> output = new ArrayList<Tuple>();
		while (temp < table.getNumOfBlocks()) {
			subListIndexes.add(temp);
			temp += memory.getMemorySize();
		}
		// clear memory
		for (int i = 0; i < memory.getMemorySize(); i++) {
			Block block = memory.getBlock(i);
			block.clear();
		}
		ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();
		// get tuples from first block of each sublist
		for (int i = 0; i < subListIndexes.size(); i++) {
			table.getBlock(subListIndexes.get(i), i);
			Block block = memory.getBlock(i);
			tuples.add(block.getTuples());
		}

		Tuple[] minTuples = new Tuple[subListIndexes.size()];
		int[] blocksReadFromSublist = new int[subListIndexes.size()];
		Arrays.fill(blocksReadFromSublist, 1);
		for (int i = 0; i < table.getNumOfTuples(); i++) {
			// If any block is empty, read the next block from that sublist
			for (int j = 0; j < subListIndexes.size(); j++) {
				if (tuples.get(j).isEmpty()) {
					if ((j < subListIndexes.size() - 1 && blocksReadFromSublist[j] < memory.getMemorySize())
							|| (j == subListIndexes.size() - 1 && blocksReadFromSublist[j] < lastBlockIndex)) {
						table.getBlock(subListIndexes.get(j) + blocksReadFromSublist[j], j);
						Block block = memory.getBlock(j);
						tuples.get(j).addAll(block.getTuples());
						blocksReadFromSublist[j]++;
					}
				}
				if (!tuples.get(j).isEmpty())
					minTuples[j] = tuples.get(j).get(0);
				else
					minTuples[j] = null;
			}

			ArrayList<Tuple> minTuplesList = new ArrayList<Tuple>(Arrays.asList(minTuples));
			Tuple minTuple = Collections.min(minTuplesList, new Comparator<Tuple>() {

				public int compare(Tuple t1, Tuple t2) {
					if (t1 == null)
						return 1;
					if (t2 == null)
						return -1;
					String val1 = t1.getField(orderField).toString();
					String val2 = t2.getField(orderField).toString();
					if (isInteger(val1) && isInteger(val2)) {
						return Integer.compare(Integer.parseInt(val1), Integer.parseInt(val2));
					} else
						return val1.compareTo(val2);
				}
			});

			int resultIndex = minTuplesList.indexOf(minTuple);
			tuples.get(resultIndex).remove(0);
			output.add(minTuple);
		}

		return output;
	}

	private static int twoPassPhaseOne(Relation table, MainMemory memory, String orderField) {
		int blocksToRead = 0, sortedBlocks = 0;
		while (sortedBlocks < table.getNumOfBlocks()) {
			if (table.getNumOfBlocks() - sortedBlocks > memory.getMemorySize())
				blocksToRead = memory.getMemorySize();
			else
				blocksToRead = table.getNumOfBlocks() - sortedBlocks;

			table.getBlocks(sortedBlocks, 0, blocksToRead);
			ArrayList<Tuple> tuples = memory.getTuples(0, blocksToRead);
			onePassSort(tuples, orderField);
			memory.setTuples(0, tuples);
			table.setBlocks(sortedBlocks, 0, blocksToRead);
			sortedBlocks += blocksToRead;
		}
		return blocksToRead;
	}

	public static List<Tuple> removeDuplicatesTwoPass(SchemaManager schemaManager, MainMemory memory, Relation table,
			List<String> selectColumnList) {
		// Phase 1
		int lastBlockIndex = twoPassPhaseOne(table, memory, null);

		// Phase 2
		int temp = 0;
		ArrayList<Integer> subListIndexes = new ArrayList<Integer>();
		ArrayList<Tuple> output = new ArrayList<Tuple>();
		while (temp < table.getNumOfBlocks()) {
			subListIndexes.add(temp);
			temp += memory.getMemorySize();
		}

		for (int i = 0; i < memory.getMemorySize(); i++) {
			Block block = memory.getBlock(i);
			block.clear();
			memory.setBlock(i, block);
		}

		ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();

		// get tuples from first block of each sublist
		for (int i = 0; i < subListIndexes.size(); i++) {
			table.getBlock(subListIndexes.get(i), i);
			Block block = memory.getBlock(i);
			tuples.add(block.getTuples());
		}

		Tuple[] minTuples = new Tuple[subListIndexes.size()];
		int[] blocksReadFromSublist = new int[subListIndexes.size()];
		Arrays.fill(blocksReadFromSublist, 1);
		for (int i = 0; i < table.getNumOfTuples(); i++) {
			for (int j = 0; j < subListIndexes.size(); j++) {
				if (tuples.get(j).isEmpty()) {
					if ((j < subListIndexes.size() - 1 && blocksReadFromSublist[j] < memory.getMemorySize())
							|| (j == subListIndexes.size() - 1 && blocksReadFromSublist[j] < lastBlockIndex)) {
						table.getBlock(subListIndexes.get(j) + blocksReadFromSublist[j], j);
						Block block = memory.getBlock(j);
						tuples.get(j).addAll(block.getTuples());
						blocksReadFromSublist[j]++;
					}
				}
				if (!tuples.get(j).isEmpty())
					minTuples[j] = tuples.get(j).get(0);
				else
					minTuples[j] = null;
			}

			ArrayList<Tuple> minTuplesList = new ArrayList<Tuple>(Arrays.asList(minTuples));
			Tuple minVal = Collections.min(minTuplesList, new Comparator<Tuple>() {
				public int compare(Tuple t1, Tuple t2) {
					int[] result = new int[selectColumnList.size()];
					if (t1 == null)
						return 1;
					if (t2 == null)
						return -1;
					for (int i = 0; i < selectColumnList.size(); i++) {
						String v1 = t1.getField(selectColumnList.get(i)).toString();
						String v2 = t2.getField(selectColumnList.get(i)).toString();
						if (isInteger(v1) && isInteger(v2)) {
							result[i] = Integer.parseInt(v1) - Integer.parseInt(v2);
						} else
							result[i] = v1.compareTo(v2);
					}
					for (int i = 0; i < selectColumnList.size(); i++) {
						if (result[i] > 0)
							return 1;
						else if (result[i] < 0)
							return -1;
					}
					return 0;
				}
			});
			Tuple comparator = null;
			int resultIndex = minTuplesList.indexOf(minVal);
			if (!isEqual(minVal, comparator, selectColumnList)) {
				output.add(minVal);
				comparator = minVal;
			}
			tuples.get(resultIndex).remove(0);
		}

		return output;
	}

	public static List<Tuple> naturalJoin(SchemaManager schemaManager, MainMemory memory, String table1, String table2,
			String column) {
		Relation t1 = schemaManager.getRelation(table1);
		Relation t2 = schemaManager.getRelation(table2);

		List<Tuple> joinedTuples;
		if (((t1.getNumOfBlocks() <= t2.getNumOfBlocks()) && (t1.getNumOfBlocks() < memory.getMemorySize() - 1))
				|| ((t2.getNumOfBlocks() < t1.getNumOfBlocks())
						&& (t2.getNumOfBlocks() < memory.getMemorySize() - 1))) {
			joinedTuples = naturalJoinOnePass(schemaManager, memory, t1, t2, column);
		} else {
			joinedTuples = naturalJoinTwoPass(schemaManager, memory, t1, t2, column);
		}

		return joinedTuples;
	}

	private static List<Tuple> naturalJoinOnePass(SchemaManager schemaManager, MainMemory memory, Relation t1,
			Relation t2, String column) {
		Relation smallerTable = null, largerTable = null;
		if (t1.getNumOfBlocks() <= t2.getNumOfBlocks()) {
			smallerTable = t1;
			largerTable = t2;
		} else {
			smallerTable = t2;
			largerTable = t1;
		}
		smallerTable.getBlocks(0, 0, smallerTable.getNumOfBlocks());

		Schema joinedSchema = combineSchema(schemaManager, t1, t2);
		String tempTableName = t1.getRelationName() + "_natural_" + t2.getRelationName() + "_temp";
		if (schemaManager.relationExists(tempTableName))
			schemaManager.deleteRelation(tempTableName);
		Relation tempTable = schemaManager.createRelation(tempTableName, joinedSchema);

		List<Tuple> output = new ArrayList<Tuple>();
		for (int i = 0; i < largerTable.getNumOfBlocks(); i++) {
			largerTable.getBlock(i, memory.getMemorySize() - 1);
			Block block = memory.getBlock(memory.getMemorySize() - 1);
			for (Tuple tup : block.getTuples()) {
				for (int j = 0; j < smallerTable.getNumOfBlocks(); j++) {
					Block block1 = memory.getBlock(j);
					for (Tuple tup1 : block1.getTuples()) {
						String s1 = tup1.getField(column).toString();
						String s2 = tup.getField(column).toString();
						if (isInteger(s1) && isInteger(s2)) {
							if (Integer.parseInt(s1) == Integer.parseInt(s2)) {
								if (smallerTable == t1)
									output.add(mergeTuple(schemaManager, tempTable, tup1, tup));
								else
									output.add(mergeTuple(schemaManager, tempTable, tup, tup1));
							}
						} else if (s1.equals(s2)) {
							if (Integer.parseInt(s1) == Integer.parseInt(s2)) {
								if (smallerTable == t1)
									output.add(mergeTuple(schemaManager, tempTable, tup1, tup));
								else
									output.add(mergeTuple(schemaManager, tempTable, tup, tup1));
							}
						}
					}
				}
			}
		}
		return output;
	}

	private static List<Tuple> naturalJoinTwoPass(SchemaManager schemaManager, MainMemory memory, Relation table1,
			Relation table2, String column) {
		Schema joinedSchema = combineSchema(schemaManager, table1, table2);
		String tempTableName = table1.getRelationName() + "_natural_" + table2.getRelationName() + "_temp";
		if (schemaManager.relationExists(tempTableName))
			schemaManager.deleteRelation(tempTableName);
		Relation tempTable = schemaManager.createRelation(tempTableName, joinedSchema);

		// Phase 1
		int lastBlock1 = twoPassPhaseOne(table1, memory, column);
		int lastBlock2 = twoPassPhaseOne(table2, memory, column);

		// Phase 2
		int temp = 0;
		List<Integer> subListIndexes1 = new ArrayList<Integer>();
		List<Integer> subListIndexes2 = new ArrayList<Integer>();
		while (temp < table1.getNumOfBlocks()) {
			subListIndexes1.add(temp);
			temp += memory.getMemorySize();
		}

		temp = 0;
		while (temp < table2.getNumOfBlocks()) {
			subListIndexes2.add(temp);
			temp += memory.getMemorySize();
		}

		for (int i = 0; i < memory.getMemorySize(); i++) {
			Block block = memory.getBlock(i);
			block.clear();
		}
		int[] blocksReadFromSublist1 = new int[subListIndexes1.size()];
		int[] blocksReadFromSublist2 = new int[subListIndexes2.size()];
		Arrays.fill(blocksReadFromSublist1, 1);
		Arrays.fill(blocksReadFromSublist2, 1);

		ArrayList<ArrayList<Tuple>> tuples1 = new ArrayList<ArrayList<Tuple>>();
		ArrayList<ArrayList<Tuple>> tuples2 = new ArrayList<ArrayList<Tuple>>();
		for (int i = 0; i < subListIndexes1.size(); i++) {
			table1.getBlock(subListIndexes1.get(i), i);
			Block block = memory.getBlock(i);
			tuples1.add(block.getTuples());
		}
		for (int i = 0; i < subListIndexes2.size(); i++) {
			table2.getBlock(subListIndexes2.get(i), i + subListIndexes1.size());
			Block block = memory.getBlock(i + subListIndexes1.size());
			tuples2.add(block.getTuples());
		}

		Tuple[] minTuple1 = new Tuple[subListIndexes1.size()];
		Tuple[] minTuple2 = new Tuple[subListIndexes2.size()];

		ArrayList<Tuple> output = new ArrayList<Tuple>();
		while (!isEmptyLists(tuples1) && !isEmptyLists(tuples2)) {
			for (int j = 0; j < subListIndexes1.size(); j++) {
				if (tuples1.get(j).isEmpty()) {
					if ((j < subListIndexes1.size() - 1 && blocksReadFromSublist1[j] < memory.getMemorySize())
							|| (j == subListIndexes1.size() - 1 && blocksReadFromSublist1[j] < lastBlock1)) {
						table1.getBlock(subListIndexes1.get(j) + blocksReadFromSublist1[j], j);
						Block block = memory.getBlock(j);
						tuples1.get(j).addAll(block.getTuples());
						blocksReadFromSublist1[j]++;
					}
				}
				if (!tuples1.get(j).isEmpty())
					minTuple1[j] = tuples1.get(j).get(0);
				else
					minTuple1[j] = null;
			}

			for (int j = 0; j < subListIndexes2.size(); j++) {
				if (tuples2.get(j).isEmpty()) {
					if ((j < subListIndexes2.size() - 1 && blocksReadFromSublist2[j] < memory.getMemorySize())
							|| (j == subListIndexes2.size() - 1 && blocksReadFromSublist2[j] < lastBlock2)) {
						table2.getBlock(subListIndexes2.get(j) + blocksReadFromSublist2[j], j + subListIndexes1.size());
						Block block = memory.getBlock(j + subListIndexes1.size());
						tuples2.get(j).addAll(block.getTuples());
						blocksReadFromSublist2[j]++;
					}
				}
				if (!tuples2.get(j).isEmpty())
					minTuple2[j] = tuples2.get(j).get(0);
				else
					minTuple2[j] = null;
			}

			// pick minimum from all sublists
			ArrayList<Tuple> minTuplesList1 = new ArrayList<>(Arrays.asList(minTuple1));
			ArrayList<Tuple> minTuplesList2 = new ArrayList<>(Arrays.asList(minTuple2));
			Tuple minVal1 = Collections.min(minTuplesList1, new Comparator<Tuple>() {
				public int compare(Tuple t1, Tuple t2) {
					if (t1 == null)
						return 1;
					if (t2 == null)
						return -1;
					String s1 = t1.getField(column).toString();
					String s2 = t2.getField(column).toString();
					if (isInteger(s1) && isInteger(s2)) {
						return (Integer.parseInt(s1) - Integer.parseInt(s2));
					} else {
						return s1.compareTo(s2);
					}
				}
			});
			Tuple minVal2 = Collections.min(minTuplesList2, new Comparator<Tuple>() {
				public int compare(Tuple t1, Tuple t2) {
					if (t1 == null)
						return 1;
					if (t2 == null)
						return -1;
					String s1 = t1.getField(column).toString();
					String s2 = t2.getField(column).toString();
					if (isInteger(s1) && isInteger(s2)) {
						return (Integer.parseInt(s1) - Integer.parseInt(s2));
					} else {
						return s1.compareTo(s2);
					}
				}
			});

			String min1 = null, min2 = null;
			if (minVal1 != null) {
				min1 = minVal1.getField(column).toString();
			}
			if (minVal2 != null) {
				min2 = minVal2.getField(column).toString();
			}

			if (min1 != null && min2 != null && min1.equals(min2)) {
				// Get all the minimum tuples in both collections, cross join
				// and output
				int count1 = getMinValueCount(tuples1, column, min1);
				int count2 = getMinValueCount(tuples2, column, min2);

				ArrayList<Tuple> minTuples1 = getMinValueTuples(tuples1, column, min1);
				ArrayList<Tuple> minTuples2 = getMinValueTuples(tuples2, column, min2);

				for (int i = 0; i < count1; i++) {
					for (int j = 0; j < count2; j++)
						output.add(mergeTuple(schemaManager, tempTable, minTuples1.get(i), minTuples2.get(j)));
				}

				/*
				 * TODO Handle the case that when the tuple indexed by one field
				 * of a relation cannot be held by the memory one time, or to
				 * say, any block is full with tuples with same field, we have
				 * to keep the other relation in case for more joins
				 */

				Boolean flag1 = false, flag2 = false;
				for (int i = 0; i < subListIndexes1.size(); i++) {
					Block tmpblk = memory.getBlock(i);
					if (getBlockMinCount(tuples1.get(i), column, min1) == tmpblk.getNumTuples()) {
						flag1 = true;
						break;
					}
				}
				for (int i = 0; i < subListIndexes2.size(); i++) {
					Block tmpblk = memory.getBlock(i + subListIndexes1.size());
					if (getBlockMinCount(tuples2.get(i), column, min1) == tmpblk.getNumTuples()) {
						flag2 = true;
						break;
					}
				}

				if (flag1 && !flag2)
					tuples1 = deleteMin(tuples1, column, min1);

				if (!flag1 && flag2)
					tuples2 = deleteMin(tuples2, column, min2);

				// Normal process
				if ((flag1 && flag2) || (!flag1 && !flag2)) {
					tuples1 = deleteMin(tuples1, column, min1);
					tuples2 = deleteMin(tuples2, column, min2);
				}

			} else if (min1 != null && min2 != null) {
				if (isInteger(min1) && isInteger(min2)) {
					if ((Integer.parseInt(min1) - Integer.parseInt(min2)) < 0)
						tuples1 = deleteMin(tuples1, column, min1);
					else
						tuples2 = deleteMin(tuples2, column, min2);
				} else {
					if (min1.compareTo(min2) < 0)
						tuples1 = deleteMin(tuples1, column, min1);
					else
						tuples2 = deleteMin(tuples2, column, min2);
				}
			}
		}
		return output;
	}

	private static Schema combineSchema(SchemaManager schemaManager, Relation t1, Relation t2) {
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<FieldType> columnTypes = new ArrayList<FieldType>();
		for (String s : t1.getSchema().getFieldNames()) {
			if (s.contains("\\."))
				columnNames.add(s);
			else
				columnNames.add(t1.getRelationName() + "." + s);

		}
		for (String s : t2.getSchema().getFieldNames()) {
			if (s.contains("\\."))
				columnNames.add(s);
			else
				columnNames.add(t2.getRelationName() + "." + s);
		}

		columnTypes.addAll(t1.getSchema().getFieldTypes());
		columnTypes.addAll(t2.getSchema().getFieldTypes());

		return new Schema(columnNames, columnTypes);
	}

	private static Tuple mergeTuple(SchemaManager schemaManager, Relation tempTable, Tuple tuple1, Tuple tuple2) {
		Tuple mergedTuple = tempTable.createTuple();
		int size1 = tuple1.getNumOfFields();
		int size2 = tuple2.getNumOfFields();
		for (int i = 0; i < size1 + size2; i++) {
			if (i < size1) {
				String s = tuple1.getField(i).toString();
				if (isInteger(s))
					mergedTuple.setField(i, Integer.parseInt(s));
				else
					mergedTuple.setField(i, s);
			} else {
				String toSet = tuple2.getField(i - size1).toString();
				if (isInteger(toSet))
					mergedTuple.setField(i, Integer.parseInt(toSet));
				else
					mergedTuple.setField(i, toSet);
			}
		}
		return mergedTuple;
	}

	public static boolean isEqual(Tuple t1, Tuple t2, List<String> columns) {
		if (t1 == null)
			return false;
		if (t2 == null)
			return false;
		for (String col : columns) {
			if (!(t1.getField(col).toString()).equals(t2.getField(col).toString()))
				return false;
		}
		return true;
	}

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean isEmptyLists(ArrayList<ArrayList<Tuple>> tuples) {
		for (ArrayList<Tuple> t : tuples) {
			if (t != null && !t.isEmpty())
				return false;
		}
		return true;
	}

	public static int getBlockMinCount(ArrayList<Tuple> tuples, String column, String minVal) {
		int result = 0;
		for (Tuple t : tuples) {
			if (t.getField(column).toString().equals(minVal)) {
				result++;
			}
		}
		return result;
	}

	public static int getMinValueCount(ArrayList<ArrayList<Tuple>> tuples, String column, String minVal) {
		int result = 0;
		for (ArrayList<Tuple> tup : tuples) {
			result += getBlockMinCount(tup, column, minVal);
		}
		return result;
	}

	public static ArrayList<Tuple> getMinValueTuples(ArrayList<ArrayList<Tuple>> tuples, String column, String minVal) {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (ArrayList<Tuple> tup : tuples) {
			for (Tuple t : tup) {
				if (t.getField(column).toString().equals(minVal)) {
					result.add(t);
				}
			}
		}
		return result;
	}

	public static ArrayList<ArrayList<Tuple>> deleteMin(ArrayList<ArrayList<Tuple>> tuples, String field, String val) {
		for (ArrayList<Tuple> tup : tuples) {
			for (Tuple t : tup) {
				if (t.getField(field).toString().equals(val)) {
					tup.remove(t);
				}
			}
		}
		return tuples;
	}

	public static List<Tuple> filter(SchemaManager schemaManager, MainMemory memory, Relation table,
			StatementNode whereNode, List<String> selectColumnList) {
		List<Tuple> filtered = new ArrayList<>();
		for (int i = 0; i < table.getNumOfBlocks(); i++) {
			table.getBlock(i, 0);
			Block block = memory.getBlock(0);
			for (Tuple t : block.getTuples()) {
				if (ExpressionEvaluator.evaluateLogicalOperator(whereNode, t)) {
					filtered.add(t);
				}
			}
		}

		ArrayList<FieldType> columnTypes = new ArrayList<>();
		if (selectColumnList.size() == 1 && selectColumnList.get(0).equals("*")) {
			selectColumnList.remove(0);
			for (int i = 0; i < table.getSchema().getNumOfFields(); i++)
				selectColumnList.add(table.getSchema().getFieldName(i));
		}

		for (String s : selectColumnList) {
			columnTypes.add(table.getSchema().getFieldType(s));
		}

		ArrayList<String> columns = new ArrayList<>(selectColumnList);
		Schema newSchema = new Schema(columns, columnTypes);
		List<Tuple> output = new ArrayList<>();
		String filteredTableName = table.getRelationName() + "_filtered";
		if (schemaManager.relationExists(filteredTableName))
			schemaManager.deleteRelation(filteredTableName);

		Relation relation = schemaManager.createRelation(filteredTableName, newSchema);

		for (Tuple t : filtered) {
			Tuple tuple = relation.createTuple();
			for (String s : newSchema.getFieldNames()) {
				if (t.getField(s).type == FieldType.INT)
					tuple.setField(s, Integer.parseInt(t.getField(s).toString()));
				else
					tuple.setField(s, t.getField(s).toString());
			}
			output.add(tuple);
		}

		return output;

	}

	public static List<Tuple> removeDuplicatesOnePassWrapper(SchemaManager schemaManager, MainMemory memory,
			Relation table, List<String> selectColumnList) {
		table.getBlocks(0, 0, table.getNumOfBlocks());
		List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
		removeDuplicateTuplesOnePass(tuples, selectColumnList);
		return tuples;
	}

	public static List<Tuple> onePassSortWrapper(SchemaManager schemaManager, MainMemory memory, Relation table,
			String orderByColumn) {
		table.getBlocks(0, 0, table.getNumOfBlocks());
		List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
		onePassSort(tuples, orderByColumn);
		return tuples;
	}

	public static Relation createTableFromTuples(SchemaManager schemaManager, MainMemory memory, List<Tuple> tuples,
			String newTableName) {
		Schema schema = tuples.get(0).getSchema();
		if (schemaManager.relationExists(newTableName))
			schemaManager.deleteRelation(newTableName);
		Relation newRelation = schemaManager.createRelation(newTableName, schema);
		int blockCount = 0;
		Block block = memory.getBlock(0);
		while (!tuples.isEmpty()) {
			block.clear();
			for (int i = 0; i < schema.getTuplesPerBlock(); i++) {
				if (!tuples.isEmpty()) {
					Tuple t = tuples.get(0);
					block.setTuple(i, t);
					tuples.remove(t);
				}
			}
			newRelation.setBlock(blockCount++, 0);
		}
		return newRelation;
	}

	public static CrossJoinTablesHelper findOptimalJoinOrder(List<Map<Set<String>, CrossJoinTablesHelper>> tempTables,
			Set<String> allTables, int memorySize) {
		// if recursion is complete
		if (tempTables.get(allTables.size() - 1).containsKey(allTables)) {
			return tempTables.get(allTables.size() - 1).get(allTables);
		}
		int block = 0;
		int tuple = 0;
		int fieldNum = 0;
		int minCost = Integer.MAX_VALUE;
		List<CrossJoinTablesHelper> joinBy = null;
		List<PairOfSets> permutations = cutSet(allTables);
		for (PairOfSets pair : permutations) {
			Set<String> setOne = pair.getSet1();
			Set<String> setTwo = pair.getSet2();
			CrossJoinTablesHelper c1 = findOptimalJoinOrder(tempTables, setOne, memorySize);
			CrossJoinTablesHelper c2 = findOptimalJoinOrder(tempTables, setTwo, memorySize);
			if (c1.getCost() + c2.getCost() + calcCost(memorySize, c1.getNumBlocks(), c2.getNumBlocks()) < minCost) {
				joinBy = new ArrayList<CrossJoinTablesHelper>();
				joinBy.add(c1);
				joinBy.add(c2);
				tuple = c1.getNumTuples() * c2.getNumTuples();
				block = blocksAfterJoin(c1.getNumTuples(), c2.getNumTuples(), 8, c1.getNumFields() + c2.getNumFields());
				fieldNum = c1.getNumFields() + c2.getNumFields();
				minCost = c1.getCost() + c2.getCost() + calcCost(memorySize, c1.getNumBlocks(), c2.getNumBlocks());
			}
		}

		CrossJoinTablesHelper ret = new CrossJoinTablesHelper(allTables, block, tuple);
		ret.setJoinBy(joinBy);
		ret.setNumFields(fieldNum);
		ret.setCost(minCost);
		tempTables.get(allTables.size() - 1).put(allTables, ret);
		return ret;
	}

	public static List<PairOfSets> cutSet(Set<String> input) {
		List<PairOfSets> result = new ArrayList<PairOfSets>();
		for (int i = 1; i <= input.size() / 2; i++) {
			Set<String> tmpSet = new HashSet<>(input);
			Set<String> pickedSet = new HashSet<>();
			permutation(tmpSet, i, 0, pickedSet, result);
		}
		return result;
	}

	public static void permutation(Set<String> input, int count, int startPos, Set<String> picked,
			List<PairOfSets> result) {
		if (count == 0)
			result.add(new PairOfSets(input, picked));
		List<String> inputList = new ArrayList<String>(input);
		for (int i = startPos; i < inputList.size(); i++) {
			Set<String> inputTmp = new HashSet<String>(input);
			Set<String> pickedTmp = new HashSet<String>(picked);
			inputTmp.remove(inputList.get(i));
			pickedTmp.add(inputList.get(i));
			permutation(inputTmp, count - 1, i, pickedTmp, result);
		}
	}

	public static int blocksAfterJoin(int tupleNum1, int tupleNum2, int blockSize, int fieldPerTuple) {
		int totalTuples = tuplesAfterJoin(tupleNum1, tupleNum2);
		return totalTuples * fieldPerTuple % blockSize == 0 ? totalTuples * fieldPerTuple / blockSize
				: totalTuples * fieldPerTuple / blockSize + 1;
	}

	public static int tuplesAfterJoin(int tupleNum1, int tupleNum2) {
		return tupleNum1 * tupleNum2;
	}

	public static int calcCost(int memSize, int blockNum1, int blockNum2) {
		if (Math.min(blockNum1, blockNum2) <= memSize)
			return blockNum1 + blockNum2;
		else
			return blockNum1 * blockNum2 + Math.min(blockNum1, blockNum2);
	}

	public static void travesal(CrossJoinTablesHelper optimalJoinTables, int level) {
		for (int i = 0; i < level; i++) {
			System.out.print(" ");
		}
		for (String str : optimalJoinTables.getTables()) {
			System.out.print(str + " ");
		}
		System.out.println();
		if (optimalJoinTables.getJoinBy() != null) {
			for (CrossJoinTablesHelper cr : optimalJoinTables.getJoinBy()) {
				travesal(cr, level + 1);
			}
		}
	}

}
