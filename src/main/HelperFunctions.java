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
	                executeOrderBy(schemaManager,memory,relation,orderField,0);
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
	
	  public static Relation executeCrossJoin(SchemaManager schemaManager, MainMemory memory, ArrayList<String> tableName, int mode){
		    ArrayList<Tuple> output;
		    Relation relation;
		    if(tableName.size()==2){

		      String r1 = tableName.get(0);
		      String r2 = tableName.get(1);
		      System.out.println("SELECT: cross join from two relations\n");
		      Relation r = schemaManager.getRelation(tableName.get(0));
		      Relation s = schemaManager.getRelation(tableName.get(1));
		      // System.out.println("SELECT: print block number of relation "+relationName.get(0)+" : "+r.getNumOfBlocks());
		      // System.out.println("SELECT: print block number of relation "+relationName.get(1)+" : "+s.getNumOfBlocks()+"\n");

		      Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;

		      /*Result can be printed directly, no need to reserve memory space to write back*/
		      if(smaller.getNumOfBlocks()<memory.getMemorySize()-1){
		        System.out.println("SELECT: cross join smaller relation can fit memory\n");
		        output = onePassJoin(schemaManager,memory,tableName.get(0),tableName.get(1));
		      }else{
		        System.out.println("SELECT: cross join smaller relation cannot fit memory\n");
		        output = nestedJoin(schemaManager,memory,tableName.get(0),tableName.get(1));
		      }
		      if(mode==0){
		      /* No need to write back */
		        System.out.println(output.get(0).getSchema().fieldNamesToString());
		        for(Tuple t:output){
		          System.out.println(t);
		        }
		        return null;
		      }else{
		        Schema schema = mergeSchema(schemaManager,tableName.get(0),tableName.get(1));
		        if(!schemaManager.relationExists(r1+"cross"+r2)){
		          relation = schemaManager.createRelation(r1+"cross"+r2,schema);
		        }
		        else{
		        	schemaManager.deleteRelation(r1+"cross"+r2);
		          relation = schemaManager.createRelation(r1+"cross"+r2,schema);
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
		    }else {
		      // System.out.println("SELECT: Multiple relations join\n");
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
	  
	  public   static  Schema mergeSchema(SchemaManager schemaManager,String table1,String table2){
		    Relation t1 = schemaManager.getRelation(table1);
		    Relation t2 = schemaManager.getRelation(table2);

		    ArrayList<String> table1FieldNames = t1.getSchema().getFieldNames();
		    ArrayList<String> table2FieldNames = t2.getSchema().getFieldNames();

		    ArrayList<FieldType> table1FieldType = t1.getSchema().getFieldTypes();
		    ArrayList<FieldType> table2FieldType = t2.getSchema().getFieldTypes();

		    ArrayList<String> newFieldNames = new ArrayList<String>();
		    ArrayList<FieldType> newFieldTypes = new ArrayList<FieldType>();

		    for(String str : table1FieldNames){
		      if(!str.contains("\\.")){
		        StringBuffer sb = new StringBuffer(str);
		        sb.insert(0,table1+".");
		        newFieldNames.add(sb.toString());
		      }else{
		        newFieldNames.add(str);
		      }
		    }

		    for(String str : table2FieldNames){
		      if(!str.contains("\\.")){
		        StringBuffer sb = new StringBuffer(str);
		        sb.insert(0,table2+".");
		        newFieldNames.add(sb.toString());
		      }else{
		        newFieldNames.add(str);
		      }
		    }

		    for(FieldType ft : table1FieldType) {
		      newFieldTypes.add(ft);
		    }
		    for(FieldType ft : table2FieldType){
		      newFieldTypes.add(ft);
		    }

		    Schema schema = new Schema(newFieldNames,newFieldTypes);
		    return schema;
		  }
	  
	  public static ArrayList<Tuple> onePassJoin(SchemaManager schemaManager, MainMemory memory, String table1, String table2){
		    ArrayList<Tuple> output = new ArrayList<Tuple>();

		    Relation t1 = schemaManager.getRelation(table1);
		    Relation t2 = schemaManager.getRelation(table2);

		    Relation smaller = (t1.getNumOfBlocks()<=t2.getNumOfBlocks())?t1:t2;
		    Relation larger = (smaller==t1)?t2:t1;
		    smaller.getBlocks(0,0,smaller.getNumOfBlocks());

		    Schema schema = mergeSchema(schemaManager,table1,table2);

		    if(schemaManager.relationExists(table1+"cross"+table2+"tmp"))
		    	schemaManager.deleteRelation(table1+"cross"+table2+"tmp");
		    Relation table = schemaManager.createRelation(table1+"cross"+table2+"tmp",schema);

		    for(int j=0;j<larger.getNumOfBlocks();j++){
		      larger.getBlock(j,memory.getMemorySize()-1);
		      Block largerblock = memory.getBlock(memory.getMemorySize()-1);
		      for(Tuple tuple1 : memory.getTuples(0,smaller.getNumOfBlocks())){
		        for(Tuple tuple2 : largerblock.getTuples()){
		          if(smaller==t1){
		            output.add(mergeTuple(schemaManager,table,tuple1,tuple2));
		          }else{
		            output.add(mergeTuple(schemaManager,table,tuple2,tuple1));
		          }
		        }
		      }
		    }
		    return output;
		  }
	  
	  public static ArrayList<Tuple> nestedJoin(SchemaManager schemaManager, MainMemory memory,String table1, String table2){
		    ArrayList<Tuple> output = new ArrayList<Tuple>();

		    Relation t1 = schemaManager.getRelation(table1);
		    Relation t2 = schemaManager.getRelation(table2);

		    Schema schema = mergeSchema(schemaManager,table1,table2);

		    if(schemaManager.relationExists(table1+"cross"+table2+"tmp"))
		    	schemaManager.deleteRelation(table1+"cross"+table2+"tmp");
		    Relation relation = schemaManager.createRelation(table1+"cross"+table2+"tmp",schema);

		    for(int i=0;i<t1.getNumOfBlocks();i++){
		    	t1.getBlock(i,0);
		      Block t1block = memory.getBlock(0);
		      for(int j=0;j<t2.getNumOfBlocks();j++){
		    	  t2.getBlock(j,1);
		        Block t2block = memory.getBlock(1);
		        for(Tuple tuple1 : t1block.getTuples()){
		          for(Tuple tuple2 : t2block.getTuples()){
		            output.add(mergeTuple(schemaManager,relation,tuple1,tuple2));
		          }
		        }
		      }
		    }
		    return output;
		  }

	  
	  public static Tuple mergeTuple(SchemaManager schemaManager, Relation table, Tuple t1, Tuple t2){
		    Tuple t = table.createTuple();
		    int size1 = t1.getNumOfFields();
		    int size2 = t2.getNumOfFields();
		    for(int i=0;i<size1+size2;i++){
		      if(i<size1){
		        String tupleType = t1.getField(i).toString();
		        if(isInteger(tupleType))
		          t.setField(i,Integer.parseInt(tupleType));
		        else
		          t.setField(i,tupleType);
		      }else{
		        String tupleType = t2.getField(i-size1).toString();
		        if(isInteger(tupleType))
		          t.setField(i,Integer.parseInt(tupleType));
		        else
		          t.setField(i,tupleType);
		      }
		    }
		    return t;
		  }
	  
	public static Relation executeOrderBy(SchemaManager schemaManager, MainMemory memory, Relation table, ArrayList<String> fieldList,int mode){
	    /* Only fields of 1 relation will be ordered in the test case*/
	    ArrayList<Tuple> output = new ArrayList<Tuple>();
	    if(table.getNumOfBlocks()<memory.getMemorySize()) {
	      //one pass for sort of 1 relation
	      System.out.println("SELECT: One pass for sorting on 1 relation\n");
			//List<Tuple> tuples = memory.getTuples(0, table.getNumOfBlocks());
			//List<Tuple> matchedTuples = new ArrayList<Tuple>();
	      output = onePassSort(table, memory, fieldList);
	    }else{
	      System.out.println("SELECT: Two pass for sorting on 1 relation\n");
	      output = twoPassSort(table,memory,fieldList);
	    }
	    /* Handle output */
	    if(mode==0){
	      System.out.println(table.getSchema().fieldNamesToString());
	      for(Tuple t:output)
	        System.out.println(t);
	      return null;
	    }else{
	      Schema schema = output.get(0).getSchema();
	      if(schemaManager.relationExists(table.getRelationName()+"ordered"))
	    	  schemaManager.deleteRelation(table.getRelationName()+"ordered");
	      Relation orderedtable = schemaManager.createRelation(table.getRelationName()+"ordered",schema);
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
	        orderedtable.setBlock(count++,0);
	      }
	      return orderedtable;
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
