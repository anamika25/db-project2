package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import parser.StatementNode;
import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;
import storageManager.Tuple;

/**
 * To execute insert query
 */
public class InsertExecutor {

	public void execute(ExecutionParameter parameter) {
		// TODO Auto-generated method stub
		SchemaManager schemaManager= parameter.getSchemaManager();
		assert schemaManager != null;
		MainMemory memory= parameter.getMemory();
        assert memory != null;

        List<StatementNode> insertParseTree = parameter.getParseTreeRoot().getBranches();
        //List<String> colList;
        //ArrayList<String> valueList = new ArrayList<String>();
        String tableName=null;
        //ExecutionParameter col = new ExecutionParameter();
        List<StatementNode> columns = null;
        for (StatementNode statementNode: insertParseTree) {
        	String type= statementNode.getType();
            if (type.equalsIgnoreCase(Constants.TABLE))
            	tableName = statementNode.getFirstChild().getType();
            else if (type.equalsIgnoreCase(Constants.COLUMNS)) {
                columns =statementNode.getBranches();
            } else if (type.equalsIgnoreCase(Constants.VALUES)) {
                Relation relation = schemaManager.getRelation(tableName);
                Tuple tuple = relation.createTuple();
                assert columns != null;
                int i = 0;
                for (StatementNode field : columns) {
 
                    assert field.getType().equalsIgnoreCase(Constants.COLUMN_NAME);
                    assert field.getBranches().size() == 1;
                    String fieldType= field.getFirstChild().getType();
      
                    assert tuple.getSchema().getFieldType(fieldType) != null;
                    assert statementNode.getBranches().get(i).getType().equalsIgnoreCase("VALUE");
                    
                    String value = statementNode.getBranches().get(i).getFirstChild().getType();
                    
                    if(tuple.getSchema().getFieldType(fieldType).equals(FieldType.INT)) {
                        tuple.setField(fieldType, Integer.parseInt(value));
                    } else {
                        tuple.setField(fieldType, value);
                    }
                    i += 1;
                }
                appendTupleToRelation(relation, memory, 0, tuple);
            } else if (type.equalsIgnoreCase(Constants.SELECT)) {
                /**
                 * Leave blank for case INSERT FROM SELECT
                 */
                Relation tempRelation = HelperFunctions.selectHandler(schemaManager, memory, "SELECT * FROM course", 1);
                String[] tp = {"sid","homework","project","exam","grade"};
                ArrayList<String> tempList = new ArrayList<>(Arrays.asList(tp));
                HelperFunctions.insertFromSelect(schemaManager, memory, schemaManager.getRelation(tableName), tempList,tempRelation);
            }
        }
        System.out.println("INSERT COMPLETE");
        //col.value.put("COL", parameter.value.get())
        //colList = super.stringMachineHashMap.get("COL").execute()

        return;
	}
	private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
        Block block_reference;
        if (relation_reference.getNumOfBlocks()==0) {
            // System.out.print("The relation is empty" + "\n");
            // System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
            block_reference=mem.getBlock(memory_block_index);
            block_reference.clear(); //clear the block
            block_reference.appendTuple(tuple); // append the tuple
            relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
        } else {
            relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
            block_reference=mem.getBlock(memory_block_index);
            if (block_reference.isFull()) {
                // System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
                block_reference.clear(); //clear the block
                block_reference.appendTuple(tuple); // append the tuple
                // System.out.print("Write to a new block at the end of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
            } else {
                // System.out.print("(The block is not full: Append it directly)" + "\n");
                block_reference.appendTuple(tuple); // append the tuple
                // System.out.print("Write to the last block of the relation" + "\n");
                relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
            }
        }
    }
}
