package main;

import java.util.List;
import java.util.Set;

public class CrossJoinTablesHelper {
	private Set<String> tables;
	private int numTuples;
	private int numFields;
	private int numBlocks;
	private List<CrossJoinTablesHelper> joinBy;
	private int cost = Integer.MAX_VALUE;

	public CrossJoinTablesHelper(Set<String> s, int block, int tuple) {
		this.numBlocks = block;
		this.numTuples = tuple;
		this.tables = s;
	}

	public Set<String> getTables() {
		return tables;
	}

	public void setTables(Set<String> tables) {
		this.tables = tables;
	}

	public int getNumTuples() {
		return numTuples;
	}

	public void setNumTuples(int numTuples) {
		this.numTuples = numTuples;
	}

	public int getNumFields() {
		return numFields;
	}

	public void setNumFields(int numFields) {
		this.numFields = numFields;
	}

	public int getNumBlocks() {
		return numBlocks;
	}

	public void setNumBlocks(int numBlocks) {
		this.numBlocks = numBlocks;
	}

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public List<CrossJoinTablesHelper> getJoinBy() {
		return joinBy;
	}

	public void setJoinBy(List<CrossJoinTablesHelper> joinBy) {
		this.joinBy = joinBy;
	}

	@Override
	public int hashCode() {
		return tables.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return tables.equals(obj);
	}

}
