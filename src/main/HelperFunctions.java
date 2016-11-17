package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

	public static Relation twoPassSort(SchemaManager schemaManager, MainMemory memory, Relation table,
			String orderField, boolean returnTable) {

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

		if (!returnTable) {
			System.out.println(table.getSchema().fieldNamesToString());
			for (Tuple t : output)
				System.out.println(t);
			return null;
		} else {
			Schema schema = output.get(0).getSchema();
			if (schemaManager.relationExists(table.getRelationName() + "_sorted"))
				schemaManager.deleteRelation(table.getRelationName() + "_sorted");
			Relation newRelation = schemaManager.createRelation(table.getRelationName() + "_sorted", schema);
			int blockCount = 0;
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
				newRelation.setBlock(blockCount++, 0);
			}
			return newRelation;
		}
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

	public static Relation removeDuplicatesTwoPass(SchemaManager schemaManager, MainMemory memory, Relation table,
			List<String> selectColumnList, boolean returnTable) {
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

		if (!returnTable) {
			System.out.println(table.getSchema().fieldNamesToString());
			for (Tuple t : output)
				System.out.println(t);
			return null;
		} else {
			Schema schema = output.get(0).getSchema();
			if (schemaManager.relationExists(table.getRelationName() + "_sorted"))
				schemaManager.deleteRelation(table.getRelationName() + "_sorted");
			Relation newRelation = schemaManager.createRelation(table.getRelationName() + "_sorted", schema);
			int blockCount = 0;
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
				newRelation.setBlock(blockCount++, 0);
			}
			return newRelation;
		}
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

}
