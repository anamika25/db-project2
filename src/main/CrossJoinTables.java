package main;

import java.util.List;
import java.util.Set;

public class CrossJoinTables {
	private Set<String> tables;
	private int numTuples;
	private int numFields;
	private int numBlocks;
	private List<CrossJoinTables> joinBy;
	private int cost = Integer.MAX_VALUE;

	public CrossJoinTables(Set<String> s, int block, int tuple) {
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

	public List<CrossJoinTables> getJoinBy() {
		return joinBy;
	}

	public void setJoinBy(List<CrossJoinTables> joinBy) {
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
