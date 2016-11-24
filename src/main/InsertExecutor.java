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
		SchemaManager schemaManager= parameter.getSchemaManager();
		MainMemory memory= parameter.getMemory();
		
		assert schemaManager != null;
        assert memory != null;
        
        List<StatementNode> insertParseTree = parameter.getParseTreeRoot().getBranches();
        String tableName=null;
        List<StatementNode> column_details = null;
        for (StatementNode statementNode: insertParseTree) {
        	String type= statementNode.getType();
            if (type.equalsIgnoreCase(Constants.TABLE))
            	tableName = statementNode.getFirstChild().getType();
            else if (type.equalsIgnoreCase(Constants.COLUMNS)) {
            	column_details =statementNode.getBranches();
            } else if (type.equalsIgnoreCase(Constants.VALUES)) {
                Relation relation = schemaManager.getRelation(tableName);
                Tuple tuple = relation.createTuple();
                assert column_details != null;
                int i = 0;
                for (StatementNode field : column_details) {
                	String fieldType= field.getFirstChild().getType();
                	
                    assert field.getType().equalsIgnoreCase(Constants.COLUMN_NAME);
                    assert field.getBranches().size() == 1;
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
                Relation tempRelation = HelperFunctions.selectHandler(schemaManager, memory, "SELECT * FROM course", 1);
                String[] tp = {"sid","homework","project","exam","grade"};
                ArrayList<String> tempList = new ArrayList<>(Arrays.asList(tp));
                HelperFunctions.insertFromSelect(schemaManager, memory, schemaManager.getRelation(tableName), tempList,tempRelation);
            }
        }
        System.out.println("INSERT COMPLETE");
        return;
	}
	
	private static void appendTupleToRelation(Relation relation, MainMemory memory, int memory_block_index, Tuple tuple) {
        Block block;
        if (relation.getNumOfBlocks()==0) {
            block=memory.getBlock(memory_block_index);
            //clear the block
            block.clear();
            // append the tuple
            block.appendTuple(tuple); 
            //write back to the relation
            relation.setBlock(relation.getNumOfBlocks(),memory_block_index); 
        } 
        else {
        	relation.getBlock(relation.getNumOfBlocks()-1,memory_block_index);
            block=memory.getBlock(memory_block_index);
            if (block.isFull()) 
            {
                // The block is full: Clear the memory block and append the tuple
                block.clear(); 
                block.appendTuple(tuple); 
                // Write to a new block at the end of the relation
                relation.setBlock(relation.getNumOfBlocks(),memory_block_index); 
            } 
            else 
            {
                // The block is not full: Append it directly
                block.appendTuple(tuple); 
                // Write to the last block of the relation
                relation.setBlock(relation.getNumOfBlocks()-1,memory_block_index); 
            }
        }
    }
}
