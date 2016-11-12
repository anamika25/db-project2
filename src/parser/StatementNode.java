package parser;

import java.util.ArrayList;
import java.util.List;

public class StatementNode {

	private String type;
	private List<StatementNode> branches;

	public StatementNode(String type, boolean isLeaf) {
		this.type = type;
		if (isLeaf)
			branches = null;
		else
			branches = new ArrayList<StatementNode>();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<StatementNode> getBranches() {
		return branches;
	}

	public void setBranches(List<StatementNode> branches) {
		this.branches = branches;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("(type ->");
		builder.append(type);
		builder.append(", branches ->");
		builder.append(branches);
		builder.append(")");
		return builder.toString();
	}

}
