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
	
	public static Relation selectHandler(SchemaManager schemaManager,MainMemory memory,String query, int subQuery)
	{
		query = query.replace(",","");
	    ArrayList<String> tableName = new ArrayList<String>();
	    ArrayList<String> orderField = new ArrayList<String>();
	    int whereIndex = 0, fromIndex = 0, orderIndex = 0, position=0;
	    
	    String[] parts = query.split(" ");
	    // Find position of keyword FROM
	    for(int i=0;i<parts.length;i++){
	        if(parts[i].equals(Constants.FROM)){
	        	fromIndex = i;
	          break;
	        }
	    }
	    // Find position of keyword WHERE
	    for(int i=0;i<parts.length;i++){
	      if(parts[i].equals(Constants.WHERE)){
	    	  whereIndex = i;
	        break;
	      }
	    }
	    // Find position of keyword ORDER
	    for(int i=0;i<parts.length;i++){
	      if(parts[i].equals(Constants.ORDER)){
	    	  orderIndex = i;
	        break;
	      }
	    }
	    // Find relations
	    if(whereIndex>0){
	      for(position=fromIndex+1;position<whereIndex;position++){
	        if(!parts[position].equals(",")){
	        	tableName.add(parts[position]);
	        }
	      }
	    }else if(orderIndex>0){
	      for(position=fromIndex+1;position<orderIndex;position++){
	        if(!parts[position].equals(",")){
	        	tableName.add(parts[position]);
	        }
	      }
	    }else{
	      for(position=fromIndex+1;position<parts.length;position++){
	        if(!parts[position].equals(",")){
	        	tableName.add(parts[position]);
	        }
	      }
	    }
	    if(orderIndex>0){
	      // Get sort field and sort relation
	      String key = parts[orderIndex+2];
	      if(key.contains("\\.")){
	        // Pattern like relation.field
	        System.out.println("SELECT: sort by field "+key);
	        String[] temp = key.split("\\.");
	        orderField.add(temp[1]);
	      }else{
	        System.out.println("SELECT: sort by field "+key);
	        orderField.add(key);
	      }
	    }
	    if(parts[1].equals(Constants.MULTIPLICATION)){//select all
	      System.out.println("SELECT: all\n");
	      if(whereIndex==0){
	        // No condition
	        System.out.println("SELECT: select with No condition\n");
	        if(tableName.size()==1){
	          /* Select on one relation */
	          System.out.println("SELECT: from one relation\n");
	          Relation relation = schemaManager.getRelation(tableName.get(0));
	          if(subQuery==0){
	            if(orderIndex==0){
	              System.out.println("SELECT: plain table scan\n");
	                /* If not a subquery and no order, plain table scan */
	                for(int i=0;i<relation.getNumOfBlocks();i++){
	                  relation.getBlock(i,0);
	                  System.out.println(memory.getBlock(0));
	                }
	                return null;
	              }
	              else{
	                /* If is order by */
	                System.out.println("SELECT: ordered table scan\n");
	                String key = parts[orderIndex+2];
	                executeOrderBy(schemaManager,memory,relation,orderField,0,key);
	                return null;
	              }
	            }else{
	              if(orderIndex==0){
	                return relation;
	              }else {
	                relation = executeOrderBy(schemaManager,memory,relation,orderField, 1);
	                return relation;
	              }
	          }
	        }else if(tableName.size()>1){
	          /* Join of Multiple relations */
	          if(subQuery==0){
	            if(orderIndex==0){
//	              executeCrossJoin(schema_manager,mem,relationName,0);
	              executeNaturalJoin(schemaManager,memory,tableName.get(0),tableName.get(1),"b",0);
	              return null;
	              // mode 0 means the join result can be printed immediately
	            }else{
	              Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	              executeOrderBy(schemaManager,memory,relation,orderField,0);
	              return null;
	            }
	          }
	          else{
	            if(orderIndex==0){
	              Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	              return  relation;
	              // Need to be written back to disk
	            }else{
	              Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	              relation = executeOrderBy(schemaManager,memory,relation,orderField,0);
	              return relation;
	            }
	          }
	        }
	      }else if(whereIndex>0){
	        // With condition
	        System.out.println("SELECT: select With conditions\n");
	      }
	    }else if(parts[1].equals("DISTINCT")){  /* For case: select distinct ...*/
	      if(parts[2].equals("*")){
	        System.out.println("SELECT: DISTINCT all\n");
	        if(fromIndex>0 && whereIndex==0){
	          // No condition
	          System.out.println("No condition\n");
	          if(tableName.size()==1){
	            //ArrayList<Tuple> output = new ArrayList<Tuple>();
	            Relation relation = schemaManager.getRelation(tableName.get(0));
	            // Only select one relation
	            System.out.println("SELECT: DISTINCT from one relation\n");
	            if(subQuery==0) {
	              if (orderIndex == 0) {
	                executeDistinct(schemaManager,memory,relation,relation.getSchema().getFieldNames(),0);
	                return null;
	              } else {
	                relation = executeDistinct(schemaManager,memory,relation,relation.getSchema().getFieldNames(),1);
	                executeOrderBy(schemaManager,memory,relation,orderField,0);
	                return null;
	              }
	            }else{
	              if (orderIndex == 0) {
	                relation = executeDistinct(schemaManager, memory, relation, relation.getSchema().getFieldNames(), 1);
	                return relation;
	              }else{
	                relation = executeDistinct(schemaManager,memory,relation,relation.getSchema().getFieldNames(),1);
	                relation = executeOrderBy(schemaManager,memory,relation,orderField,0);
	                return relation;
	              }
	            }
	          }else if(tableName.size()>1){
	            /* Join of Multiple relations */
	            if(subQuery==0){
	              if(orderIndex==0){
	                Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	                executeDistinct(schemaManager,memory,relation,tableName,0);
	                return null;
	                // mode 0 means the join result can be printed immediately
	              }else{
	                Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	                relation = executeDistinct(schemaManager,memory,relation,tableName,1);
	                executeOrderBy(schemaManager,memory,relation,orderField,0);
	                return null;
	              }
	            }
	            else{
	              if(orderIndex==0){
	                Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	                relation = executeDistinct(schemaManager,memory,relation,tableName,1);
	                return  relation;
	                // Need to be written back to disk
	              }else{
	                Relation relation = executeCrossJoin(schemaManager,memory,tableName,1);
	                relation = executeDistinct(schemaManager,memory,relation,tableName,1);
	                relation = executeOrderBy(schemaManager,memory,relation,orderField,1);
	                return relation;
	              }
	            }
	          }
	        }else if(whereIndex>0){
	          // With condition
	          System.out.println("With condition\n");
	          return null;
	        }
	      }else{
	        return null;
	      }
	    }else{
	      return null;
	    }
	    return null;
	}
	
	  public static Relation executeCrossJoin(SchemaManager schemaManager, MainMemory memory, ArrayList<String> tableNames, boolean returnTable){
		    
		    if(!tableNames.isEmpty() && tableNames.size()==2){
		      String table1 = tableNames.get(0);
		      String table2 = tableNames.get(1);
		      System.out.println("Applying cross join");
		      Relation r = schemaManager.getRelation(table1);
		      Relation s = schemaManager.getRelation(table2);

		      Relation smallerTable = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;

		      ArrayList<Tuple> output;
		      if(smallerTable.getNumOfBlocks()<memory.getMemorySize()-1){
		        output = onePassJoin(schemaManager,memory,r,s);
		      }else{
		        output = nestedJoin(schemaManager,memory,r,s);
		      }
		      if(!returnTable){
		        System.out.println(output.get(0).getSchema().fieldNamesToString());
		        for(Tuple t:output){
		          System.out.println(t);
		        }
		        return null;
		      }else{
		        Schema schema = combineSchema(schemaManager,r,s);
		        String crossTableName = table1+"_cross_"+table2;
				if(schemaManager.relationExists(crossTableName)){
					schemaManager.deleteRelation(crossTableName);
		        }
				Relation table = schemaManager.createRelation(crossTableName,schema);

				int count = 0;
		        Block block = memory.getBlock(0);
		        while(!output.isEmpty()){
		        	block.clear();
		          for(int i=0;i<schema.getTuplesPerBlock();i++){
		            if(!output.isEmpty()){
		              Tuple t = output.get(0);
		              block.setTuple(i,t);
		              output.remove(t);
		            }
		          }
		          table.setBlock(count++,0);
		        }
		        return table;
		      }
		    }else {
		      return null;
		    }
		  }
	
	  public static Relation executeNaturalJoin(SchemaManager schemaManager, MainMemory memory, String r1, String r2, String field, int mode){
		    ArrayList<Tuple> output;
		    Relation relation;

		    System.out.println("SELECT: natural join from two relations\n");
		    Relation r = schemaManager.getRelation(r1);
		    Relation s = schemaManager.getRelation(r2);
		    // System.out.println("SELECT: print block number of relation "+r1+" : "+r.getNumOfBlocks());
		    // System.out.println("SELECT: print block number of relation "+r2+" : "+s.getNumOfBlocks()+"\n");

		    Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;

		    /*Result can be printed directly, no need to reserve memory space to write back*/
		    if(smaller.getNumOfBlocks()<memory.getMemorySize()-1) {
		      System.out.println("SELECT: natural join smaller relation can fit memory\n");
		      output = onePassNaturalJoin(schemaManager,memory,r1,r2,field);
		    }else{
		      System.out.println("SELECT: natural join smaller relation cannot fit memory\n");
		      output = twoPassNaturalJoin(schemaManager,memory,r1,r2,field);
		    }
		    /*Determine output*/
		    if(mode==0) {
		      /* No need to write back, print out directly*/
		      System.out.println(output.get(0).getSchema().fieldNamesToString());
		      for (Tuple t : output) {
		        System.out.println(t);
		      }
		      return null;
		    }else{
		      /*Should write back to disk*/
		      Schema schema = mergeSchema(schemaManager,r1,r2);
		      if(!schemaManager.relationExists(r1+"natural"+r2)){
		        relation = schemaManager.createRelation(r1+"natural"+r2,schema);
		      }
		      else{
		    	  schemaManager.deleteRelation(r1+"natural"+r2);
		        relation = schemaManager.createRelation(r1+"natural"+r2,schema);
		      }
		        /*Need to be optimized to get block I/O*/
		      int count = 0;
		      Block block = memory.getBlock(0);
		      while(!output.isEmpty()){
		    	  block.clear();
		        for(int i=0;i<schema.getTuplesPerBlock();i++){
		          if(!output.isEmpty()){
		            Tuple t = output.get(0);
		            block.setTuple(i,t);
		            output.remove(t);
		          }
		        }
		        relation.setBlock(count++,0);
		      }
		      return relation;
		    }
		  }
	  
	  
	  public static ArrayList<Tuple> onePassJoin(SchemaManager schemaManager, MainMemory memory, Relation table1, Relation table2){

		    Relation smaller = (table1.getNumOfBlocks()<=table2.getNumOfBlocks())?table1:table2;
		    Relation larger = (smaller==table1)?table2:table1;
		    smaller.getBlocks(0,0,smaller.getNumOfBlocks());

		    Schema schema = combineSchema(schemaManager,table1,table2);

		    String tempTableName = table1+"_cross_"+table2+"_tmp";
			if(schemaManager.relationExists(tempTableName))
		    	schemaManager.deleteRelation(tempTableName);
		    Relation table = schemaManager.createRelation(tempTableName,schema);

		    ArrayList<Tuple> output = new ArrayList<Tuple>();
		    for(int j=0;j<larger.getNumOfBlocks();j++){
		      larger.getBlock(j,memory.getMemorySize()-1);
		      Block largerblock = memory.getBlock(memory.getMemorySize()-1);
		      for(Tuple tuple1 : memory.getTuples(0,smaller.getNumOfBlocks())){
		        for(Tuple tuple2 : largerblock.getTuples()){
		          if(smaller==table1){
		            output.add(mergeTuple(schemaManager,table,tuple1,tuple2));
		          }else{
		            output.add(mergeTuple(schemaManager,table,tuple2,tuple1));
		          }
		        }
		      }
		    }
		    return output;
		  }
	  
	  public static ArrayList<Tuple> nestedJoin(SchemaManager schemaManager, MainMemory memory,Relation table1, Relation table2){
		    Schema schema = combineSchema(schemaManager,table1,table2);
		    String tempTableName = table1+"cross"+table2+"tmp";
			if(schemaManager.relationExists(tempTableName))
		    	schemaManager.deleteRelation(tempTableName);
		    Relation table = schemaManager.createRelation(tempTableName,schema);

		    ArrayList<Tuple> output = new ArrayList<Tuple>();
		    for(int i=0;i<table1.getNumOfBlocks();i++){
		    	table1.getBlock(i,0);
		      Block t1block = memory.getBlock(0);
		      for(int j=0;j<table2.getNumOfBlocks();j++){
		    	  table2.getBlock(j,1);
		        Block t2block = memory.getBlock(1);
		        for(Tuple tuple1 : t1block.getTuples()){
		          for(Tuple tuple2 : t2block.getTuples()){
		            output.add(mergeTuple(schemaManager,table,tuple1,tuple2));
		          }
		        }
		      }
		    }
		    return output;
		  }

	public static Relation executeOrderBy(SchemaManager schemaManager, MainMemory memory, Relation table, ArrayList<String> fieldList,int mode){
	    /* Only fields of 1 relation will be ordered in the test case*/
	    ArrayList<Tuple> output = new ArrayList<Tuple>();
	    Schema schema = output.get(0).getSchema();
	    Relation orderedTable = schemaManager.createRelation(table.getRelationName()+"ordered",schema); 
	    if(table.getNumOfBlocks()<memory.getMemorySize()) {
	      System.out.println("SELECT: One pass for sorting on 1 relation\n");
			List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
	      output = onePassSort(tuples, key);
	    }else{
	      System.out.println("SELECT: Two pass for sorting on 1 relation\n");
	      orderedTable = twoPassSort(schemaManager, memory, table,key,mode);
	      return orderedTable;
	    }
	  }
	
	public static Relation executeDistinct(SchemaManager schemaManager, MainMemory memory, Relation table, ArrayList<String> fieldList,int mode) {
	    ArrayList<Tuple> output = new ArrayList<Tuple>();
	    if (table.getNumOfBlocks() < memory.getMemorySize()) {
	      output = onePassDistinct(table, memory, fieldList);
	    } else {
	      output = twoPassDistinct(table, memory, fieldList);
	    }

	    if (mode == 0) {
	      System.out.println(table.getSchema().fieldNamesToString());
	      for (Tuple t : output)
	        System.out.println(t);
	      return null;
	    } else {
	      Schema schema = output.get(0).getSchema();
	      if(schemaManager.relationExists(table.getRelationName()+"distinct"))
	    	  schemaManager.deleteRelation(table.getRelationName()+"distinct");
	      Relation distinctTable = schemaManager.createRelation(table.getRelationName() + "distinct", schema);
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
	        distinctTable.setBlock(count++, 0);
	      }
	      return distinctTable;
	    }
	  }

	public static ArrayList<Tuple> onePassDistinct(Relation table, MainMemory memory, final ArrayList<String> indexField){
		table.getBlocks(0,0,table.getNumOfBlocks());
	    ArrayList<Tuple> tuples = memory.getTuples(0,table.getNumOfBlocks());
	    ArrayList<Tuple> output=new ArrayList<Tuple>();
	    Tuple tmp = null;
	    for(Tuple tuple : tuples){
	      tuple = Collections.min(tuples,new Comparator<Tuple>(){
	        public int compare(Tuple t1, Tuple t2){
	          if(t1==null) return 1;
	          if(t2==null) return -1;
	          int[] result = new int[indexField.size()];
	          for(int i=0;i<indexField.size();i++){
	            String v1 = t1.getField(indexField.get(i)).toString();
	            String v2 = t2.getField(indexField.get(i)).toString();
	            if(isInteger(v1) && isInteger(v2)){
	              result[i] = Integer.parseInt(v1)-Integer.parseInt(v2);
	            }
	            else
	              result[i] = v1.compareTo(v2);
	          }
	          // Return 0 when all fields equal, 1 when t1>t2, -1 when t1<t2
	          for(int i=0;i<indexField.size();i++){
	            if(result[i]>0) return 1;
	            else if(result[i]<0) return -1;
	          }
	          return 0;
	        }
	      });
	      if(!isEqual(tuple,tmp,indexField)){
	        tmp = tuple;
	        output.add(tuple);
	      }
	      tuples.remove(tuples.indexOf(tuple));
	    }
	    return output;
	  }

	  public static ArrayList<Tuple> twoPassDistinct(Relation table, MainMemory memory, final ArrayList<String> indexField){
	    int temp=0,printed=0;
	    ArrayList<Integer> segments=new ArrayList<Integer>();
	    ArrayList<Tuple> output=new ArrayList<Tuple>();
	    
	    // Phase 1
	    int last_segment = twoPassPhaseOne(table,memory,indexField);
	    // Phase 2
	    while(temp<table.getNumOfBlocks()){
	      segments.add(temp);
	      temp+=memory.getMemorySize();
	    }
	    // System.out.println("SELECT: DISTINCT by two pass: "+segments.size()+" segments\n");
	    Block block= null;
	    for(int i=0;i<memory.getMemorySize();i++){
	      block = memory.getBlock(i); //access to memory block 0
	      block.clear(); //clear the block
	      memory.setBlock(i,block);
	    }
	    int[] reads = new int[segments.size()];
	    Arrays.fill(reads,1);

	    // Initialize memory with first blocks
	    ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();
	    for(int i=0;i<segments.size();i++){
	    	table.getBlock(segments.get(i),i);
	      block = memory.getBlock(i);
	      tuples.add(block.getTuples());
	    }

	    Tuple comparator = null;
	    for(int i=0;i<table.getNumOfTuples();i++){
	      //  Test if the block is empty, if is, read in next block in the segment
	      for(int j=0;j<segments.size();j++){
	        if(tuples.get(j).isEmpty()){
	          if(j<segments.size()-1 && reads[j]<memory.getMemorySize()){
	        	  table.getBlock(segments.get(j)+reads[j],j);
	            block = memory.getBlock(j);
	            tuples.get(j).addAll(block.getTuples());
	            reads[j]++;
	          }else if(j==segments.size()-1 && reads[j]<last_segment){
	        	  table.getBlock(segments.get(j)+reads[j],j);
	            block = memory.getBlock(j);
	            tuples.get(j).addAll(block.getTuples());
	            reads[j]++;
	          }
	        }
	      }
	      Tuple[] minTuple = new Tuple[segments.size()];
	      for(int k=0;k<segments.size();k++){
	        if(!tuples.get(k).isEmpty()){
	           minTuple[k] = Collections.min(tuples.get(k),new Comparator<Tuple>(){
	            public int compare(Tuple t1, Tuple t2){
	              int[] result = new int[indexField.size()];
	              if(t1==null) return 1;
	              if(t2==null) return -1;
	              for(int f=0;f<indexField.size();f++){
	                String v1 = t1.getField(indexField.get(f)).toString();
	                String v2 = t2.getField(indexField.get(f)).toString();
	                if(isInteger(v1) && isInteger(v2)){
	                  result[f] = Integer.parseInt(v1)-Integer.parseInt(v2);
	                }
	                else
	                  result[f] = v1.compareTo(v2);
	                }
	              // Return 0 when all fields equal, 1 when t1>t2, -1 when t1<t2
	              for(int f=0;f<indexField.size();f++){
	                if(result[f]>0) return 1;
	                else if(result[f]<0) return -1;
	              }
	              return 0;
	            }
	          });
	        }else{
	          minTuple[k] = null;
	        }
	      }
	      ArrayList<Tuple> tmp = new ArrayList<Tuple>(Arrays.asList(minTuple));
	      
	      Tuple minVal = Collections.min(tmp,new Comparator<Tuple>(){
	          public int compare(Tuple t1, Tuple t2){
	              int[] result = new int[indexField.size()];
	              if(t1==null) return 1;
	              if(t2==null) return -1;
	              for(int f=0;f<indexField.size();f++){
	                String v1 = t1.getField(indexField.get(f)).toString();
	                String v2 = t2.getField(indexField.get(f)).toString();
	                if(isInteger(v1) && isInteger(v2)){
	                  result[f] = Integer.parseInt(v1)-Integer.parseInt(v2);
	                }
	                else
	                  result[f] = v1.compareTo(v2);
	              }
	              // Return 0 when all fields equal, 1 when t1>t2, -1 when t1<t2
	              for(int f=0;f<indexField.size();f++){
	                if(result[f]>0) return 1;
	                else if(result[f]<0) return -1;
	              }
	              return 0;
	            }
	      });
	      int resultIndex = tmp.indexOf(minVal);
	      int tupleIndex = tuples.get(resultIndex).indexOf(minTuple[resultIndex]);
	      if(!isEqual(minVal,comparator,indexField)){
	        output.add(minVal);
	        comparator = minVal;
	        printed++;
	      }
	      tuples.get(resultIndex).remove(tupleIndex);
	    }
	    System.out.println("SELECT: Two Pass Duplicate Elimination: "+printed+" tuples printed\n");
	    // System.out.print("Now the memory contains: " + "\n");
	    // System.out.print(mem + "\n");
	    return output;
	  }

	  public static int twoPassPhaseOne(Relation table, MainMemory memory, final ArrayList<String> indexField){
		    int readIn=0, sortedBlocks = 0;
		    while(sortedBlocks<table.getNumOfBlocks()){
		      readIn = ((table.getNumOfBlocks()-sortedBlocks)>memory.getMemorySize())?memory.getMemorySize():(table.getNumOfBlocks()-sortedBlocks);
		      table.getBlocks(sortedBlocks,0,readIn);
		      ArrayList<Tuple> tuples = memory.getTuples(0,readIn);
		      Collections.sort(tuples,new Comparator<Tuple>(){
		          public int compare(Tuple t1, Tuple t2){
		            int[] result = new int[indexField.size()];
		            for(int i=0;i<indexField.size();i++){
		              String v1 = t1.getField(indexField.get(i)).toString();
		              String v2 = t2.getField(indexField.get(i)).toString();
		              if(isInteger(v1) && isInteger(v2)){
		                result[i] = Integer.parseInt(v1)-Integer.parseInt(v2);
		              }
		              else
		                result[i] = v1.compareTo(v2);
		            }
		            // Return 0 when all fields equal, 1 when t1>t2, -1 when t1<t2
		            for(int i=0;i<indexField.size();i++){
		              if(result[i]>0) return 1;
		              else if(result[i]<0) return -1;
		            }
		            return 0;
		          }
		      });
		      memory.setTuples(0,tuples);
		      //System.out.print("Now the memory contains: " + "\n");
		      //System.out.print(mem + "\n");
		      table.setBlocks(sortedBlocks,0,readIn);
		      sortedBlocks+=readIn;
		    }
		    return readIn;
		  }
	  
	public static void insertFromSelect(SchemaManager schemaManager,MainMemory memory, Relation table, ArrayList<String> fields, Relation tempRelation)
	{
		ArrayList<FieldType> fieldTypes = new ArrayList<>();
	    if(fields.size()==1 && fields.get(0).equals(Constants.MULTIPLICATION)){
	      fields.remove(0);
	      for(int i=0;i<tempRelation.getSchema().getNumOfFields();i++){
	        fields.add(tempRelation.getSchema().getFieldName(i));
	      }
	    }

	    for(String s:fields){
	      fieldTypes.add(tempRelation.getSchema().getFieldType(s));
	    }
	    Schema schema = new Schema(fields,fieldTypes);
	    if(schemaManager.relationExists("fromSelect"))
	    	schemaManager.deleteRelation("fromSelect");

	    Relation relation = schemaManager.createRelation("fromSelect",schema);
	    Tuple tuple = relation.createTuple();
	    ArrayList<Tuple> output = new ArrayList<>();

	    for(int i=0;i<tempRelation.getNumOfBlocks();i++){
	    	tempRelation.getBlock(i,0);
	      Block block = memory.getBlock(0);
	      for(Tuple t:block.getTuples()){
	        for(int j=0;j<fields.size();j++){
	          if(t.getField(fields.get(j)).type==FieldType.INT)
	            tuple.setField(j,Integer.parseInt(t.getField(fields.get(j)).toString()));
	          else
	            tuple.setField(j,t.getField(fields.get(j)).toString());
	        }
	        output.add(tuple);
	      }
	    }
	    /* Insert back to disk */
	    /*Need to be optimized to get block I/O*/
	    int count = table.getNumOfBlocks();
	    Block block = memory.getBlock(0);
	    while(!output.isEmpty()){
	    	block.clear();
	      for(int i=0;i<schema.getTuplesPerBlock();i++){
	        if(!output.isEmpty()){
	          Tuple t = output.get(0);
	          block.setTuple(i,t);
	          output.remove(t);
	        }
	      }
	      table.setBlock(count++,0);
	    }
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

	public static Relation naturalJoin(SchemaManager schemaManager, MainMemory memory, String table1, String table2,
			String column, boolean returnTable) {
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

		if (!returnTable) {
			System.out.println(joinedTuples.get(0).getSchema().fieldNamesToString());
			for (Tuple t : joinedTuples)
				System.out.println(t);
			return null;
		} else {
			Schema schema = joinedTuples.get(0).getSchema();
			String joinTableName = table1 + "_natural_" + table2;
			if (schemaManager.relationExists(joinTableName))
				schemaManager.deleteRelation(joinTableName);
			Relation newRelation = schemaManager.createRelation(joinTableName, schema);

			int blockCount = 0;
			Block block = memory.getBlock(0);
			while (!joinedTuples.isEmpty()) {
				block.clear();
				for (int i = 0; i < schema.getTuplesPerBlock(); i++) {
					if (!joinedTuples.isEmpty()) {
						Tuple t = joinedTuples.get(0);
						block.setTuple(i, t);
						joinedTuples.remove(t);
					}
				}
				newRelation.setBlock(blockCount++, 0);
			}
			return newRelation;
		}
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

	private static Tuple mergeTuple(SchemaManager schemaManager, Relation tempTable, Tuple tup1, Tuple tup2) {
		Tuple mergedTuple = tempTable.createTuple();
		int size1 = tup1.getNumOfFields();
		int size2 = tup2.getNumOfFields();
		for (int i = 0; i < size1 + size2; i++) {
			if (i < size1) {
				String s = tup1.getField(i).toString();
				if (isInteger(s))
					mergedTuple.setField(i, Integer.parseInt(s));
				else
					mergedTuple.setField(i, s);
			} else {
				String toSet = tup2.getField(i - size1).toString();
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

	public static Relation filter(SchemaManager schemaManager, MainMemory memory, Relation table,
			StatementNode whereNode, List<String> selectColumnList, boolean returnTable) {
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

		if (!returnTable) {
			System.out.println(newSchema.fieldNamesToString());
			for (Tuple t : output) {
				System.out.println(t);
			}
			return null;
		} else {
			int blockCount = 0;
			Block block = memory.getBlock(0);
			while (!output.isEmpty()) {
				block.clear();
				for (int i = 0; i < newSchema.getTuplesPerBlock(); i++) {
					if (!output.isEmpty()) {
						Tuple t = output.get(0);
						block.setTuple(i, t);
						output.remove(t);
					}
				}
				relation.setBlock(blockCount++, 0);
			}
			return relation;
		}

	}

	public static Relation removeDuplicatesOnePassWrapper(SchemaManager schemaManager, MainMemory memory,
			Relation table, List<String> selectColumnList, boolean returnTable) {
		table.getBlocks(0, 0, table.getNumOfBlocks());
		List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
		removeDuplicateTuplesOnePass(tuples, selectColumnList);
		if (!returnTable) {
			System.out.println(table.getSchema().fieldNamesToString());
			for (Tuple t : tuples)
				System.out.println(t);
			return null;
		} else {
			Schema schema = tuples.get(0).getSchema();
			String distinctTableName = table.getRelationName() + "_distinct";
			if (schemaManager.relationExists(distinctTableName))
				schemaManager.deleteRelation(distinctTableName);
			Relation newRelation = schemaManager.createRelation(distinctTableName, schema);
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
	}

	public static Relation onePassSortWrapper(SchemaManager schemaManager, MainMemory memory, Relation table,
			String orderByColumn, boolean returnTable) {
		table.getBlocks(0, 0, table.getNumOfBlocks());
		List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
		onePassSort(tuples, orderByColumn);
		if (!returnTable) {
			System.out.println(table.getSchema().fieldNamesToString());
			for (Tuple t : tuples)
				System.out.println(t);
			return null;
		} else {
			Schema schema = tuples.get(0).getSchema();
			String orderedTableName = table.getRelationName() + "_ordered";
			if (schemaManager.relationExists(orderedTableName))
				schemaManager.deleteRelation(orderedTableName);
			Relation newRelation = schemaManager.createRelation(orderedTableName, schema);
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
	}
	
	public static CrossJoinTables findOptimal(List<Map<Set<String>, CrossJoinTables>> tempRelations,
			Set<String> allTables, int memorySize) {
		// if recursion is complete
		if (tempRelations.get(allTables.size() - 1).containsKey(allTables)) {
			return tempRelations.get(allTables.size() - 1).get(allTables);
		}
		int block = 0;
		int tuple = 0;
		int fieldNum = 0;
		int minCost = Integer.MAX_VALUE;
		List<CrossJoinTables> joinBy = null;
		List<PairOfSets> permutation = cutSet(allTables);
		for (PairOfSets pair : permutation) {
			Set<String> setOne = pair.getSet1();
			Set<String> setTwo = pair.getSet2();
			CrossJoinTables c1 = findOptimal(tempRelations, setOne, memorySize);
			CrossJoinTables c2 = findOptimal(tempRelations, setTwo, memorySize);
			if (c1.getCost() + c2.getCost() + calcCost(memorySize, c1.getNumBlocks(), c2.getNumBlocks()) < minCost) {
				joinBy = new ArrayList<>();
				joinBy.add(c1);
				joinBy.add(c2);
				tuple = c1.getNumTuples() * c2.getNumTuples();
				block = blocksAfterJoin(c1.getNumTuples(), c2.getNumTuples(), 8, c1.getNumFields() + c2.getNumFields());
				fieldNum = c1.getNumFields() + c2.getNumFields();
				minCost = c1.getCost() + c2.getCost() + calcCost(memorySize, c1.getNumBlocks(), c2.getNumBlocks());
			}
		}

		CrossJoinTables ret = new CrossJoinTables(allTables, block, tuple);
		ret.setJoinBy(joinBy);
		ret.setNumFields(fieldNum);
		ret.setCost(minCost);
		tempRelations.get(allTables.size() - 1).put(allTables, ret);
		return ret;
	}

	public static List<PairOfSets> cutSet(Set<String> input) {
		List<PairOfSets> result = new ArrayList<PairOfSets>();
		for (int i = 1; i <= input.size() / 2; i++) {
			Set<String> tmpSet = new HashSet<>(input);
			Set<String> pickedSet = new HashSet<>();
			helper(tmpSet, i, 0, pickedSet, result);
		}
		return result;
	}

	public static void helper(Set<String> input, int count, int startPos, Set<String> picked, List<PairOfSets> result) {
		if (count == 0)
			result.add(new PairOfSets(input, picked));
		List<String> inputList = new ArrayList<String>(input);
		for (int i = startPos; i < inputList.size(); i++) {
			Set<String> inputTmp = new HashSet<String>(input);
			Set<String> pickedTmp = new HashSet<String>(picked);
			inputTmp.remove(inputList.get(i));
			pickedTmp.add(inputList.get(i));
			helper(inputTmp, count - 1, i, pickedTmp, result);
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
	
	public static void travesal(CrossJoinTables relations, int level) {
		for (int i = 0; i < level; i++) {
			System.out.print(" ");
		}
		for (String str : relations.getTables()) {
			System.out.print(str + " ");
		}
		System.out.println();
		if (relations.getJoinBy() != null) {
			for (CrossJoinTables cr : relations.getJoinBy()) {
				travesal(cr, level + 1);
			}
		 }
    }

}
