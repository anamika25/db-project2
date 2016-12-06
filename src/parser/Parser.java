package parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import main.Constants;
import main.HelperFunctions;

/**
 * Parse raw query and return the parse tree
 */
public class Parser {

	private static Map<String, Integer> operatorPriorityMap;

	static {
		operatorPriorityMap = new HashMap<String, Integer>();
		operatorPriorityMap.put(Constants.OR, 0);
		operatorPriorityMap.put(Constants.AND, 1);
		operatorPriorityMap.put(Constants.LESS_THAN, 2);
		operatorPriorityMap.put(Constants.GREATER_THAN, 2);
		operatorPriorityMap.put(Constants.EQUAL, 2);
		operatorPriorityMap.put(Constants.ADDITION, 3);
		operatorPriorityMap.put(Constants.SUBTRACTION, 3);
		operatorPriorityMap.put(Constants.MULTIPLICATION, 4);
	}

	public static StatementNode startParse(String query) throws ParserException {
		// System.out.println("Parsing query: " + query);
		String[] parts = query.split(" ");
		StatementNode statement = null;

		if (parts[0].equalsIgnoreCase(Constants.SELECT)) {
			statement = parseSelect(parts);
		} else if (parts[0].equalsIgnoreCase(Constants.CREATE)) {
			statement = parseCreate(parts);
		} else if (parts[0].equalsIgnoreCase(Constants.INSERT)) {
			statement = parseInsert(parts);
		} else if (parts[0].equalsIgnoreCase(Constants.DELETE)) {
			statement = parseDelete(parts);
		} else if (parts[0].equalsIgnoreCase(Constants.DROP)) {
			statement = new StatementNode(Constants.DROP, false);
			statement.getBranches().add(leaf(parts[2], Constants.TABLE));
		}
		return statement;
	}

	private static StatementNode parseSelect(String[] parts) throws ParserException {
		StatementNode statement = new StatementNode(Constants.SELECT, false);
		int fromIndex = 0, whereIndex = 0, orderByIndex = 0;
		for (int i = 1; i < parts.length; i++) {
			if (parts[i].equalsIgnoreCase(Constants.FROM))
				fromIndex = i;
			if (parts[i].equalsIgnoreCase(Constants.WHERE))
				whereIndex = i;
			if (parts[i].equalsIgnoreCase(Constants.ORDER))
				orderByIndex = i;
		}
		if (fromIndex <= 1)
			throw new ParserException("Wrong position of FROM in select statement.");
		else {
			StatementNode selectSublist = parseColumns(Arrays.copyOfRange(parts, 1, fromIndex));
			statement.getBranches().add(selectSublist);
		}

		int end = orderByIndex > 0 ? orderByIndex : parts.length;
		if (whereIndex > 0) {
			if (whereIndex < 4)
				throw new ParserException("Wrong position of WHERE in select statement.");
			StatementNode fromStatement = parseFrom(Arrays.copyOfRange(parts, fromIndex + 1, whereIndex));
			StringBuilder builder = new StringBuilder();
			for (int i = whereIndex + 1; i < end; i++) {
				builder.append(parts[i]);
			}
			StatementNode whereStatement = parseWhere(parseWhereByOperators(builder.toString()));
			statement.getBranches().add(fromStatement);
			statement.getBranches().add(whereStatement);
		} else {
			StatementNode fromStatement = parseFrom(Arrays.copyOfRange(parts, fromIndex + 1, end));
			statement.getBranches().add(fromStatement);
		}

		if (orderByIndex > 0) {
			if (orderByIndex < 4)
				throw new ParserException("Wrong position of ORDER in select statement.");
			if (!parts[orderByIndex + 1].equalsIgnoreCase("BY"))
				throw new ParserException("Wrong ORDER BY in select statement.");
			StatementNode orderStatement = parseOrderBy(Arrays.copyOfRange(parts, orderByIndex + 2, parts.length));
			statement.getBranches().add(orderStatement);
		}
		return statement;
	}

	private static StatementNode parseCreate(String[] parts) throws ParserException {
		if (!parts[1].equalsIgnoreCase(Constants.TABLE))
			throw new ParserException("TABLE keyword wrong in create statement.");
		StatementNode statement = new StatementNode(Constants.CREATE, false);
		statement.getBranches().add(leaf(parts[2], Constants.TABLE));
		statement.getBranches().add(parseCreateColumns(Arrays.copyOfRange(parts, 3, parts.length)));
		return statement;
	}

	private static StatementNode parseInsert(String[] parts) throws ParserException {
		if (!parts[1].equalsIgnoreCase("INTO"))
			throw new ParserException("INTO keyword wrong in insert statement.");
		StatementNode statement = null;
		int valuesIndex = 0, selectIndex = 0;
		for (int i = 0; i < parts.length; i++) {
			if (parts[i].equals(Constants.VALUES))
				valuesIndex = i;
			if (parts[i].equals(Constants.SELECT))
				selectIndex = i;
		}
		if (valuesIndex > 0) {
			StatementNode returnStatement = new StatementNode(Constants.INSERT, false);
			returnStatement.getBranches().add(leaf(parts[2], Constants.TABLE));
			returnStatement.getBranches().add(parseColumns(Arrays.copyOfRange(parts, 3, valuesIndex)));
			returnStatement.getBranches().add(parseValues(Arrays.copyOfRange(parts, valuesIndex + 1, parts.length)));
			statement = returnStatement;
		}

		if (selectIndex > 0) {
			StatementNode returnStatement = new StatementNode(Constants.INSERT, false);
			returnStatement.getBranches().add(leaf(parts[2], Constants.TABLE));
			returnStatement.getBranches().add(parseColumns(Arrays.copyOfRange(parts, 3, selectIndex)));
			returnStatement.getBranches().add(parseValues(Arrays.copyOfRange(parts, selectIndex, parts.length)));
			statement = returnStatement;
		}
		return statement;
	}

	private static StatementNode parseDelete(String[] parts) throws ParserException {
		if (!parts[1].equalsIgnoreCase(Constants.FROM))
			throw new ParserException("FROM keyword wrong in delete statement.");
		StatementNode statement = new StatementNode(Constants.DELETE, false);
		statement.getBranches().add(leaf(parts[2], Constants.TABLE));
		if (parts.length > 3 && parts[3].equalsIgnoreCase(Constants.WHERE)) {
			StringBuilder builder = new StringBuilder();
			for (int i = 4; i < parts.length; i++) {
				builder.append(parts[i]);
			}
			statement.getBranches().add(parseWhere(parseWhereByOperators(builder.toString())));
		}
		return statement;
	}

	private static StatementNode parseColumns(String[] columnParts) {
		StatementNode statement = new StatementNode(Constants.COLUMNS, false);
		if (columnParts[0].equalsIgnoreCase(Constants.DISTINCT)) {
			statement.getBranches().add(new StatementNode(Constants.DISTINCT, false));
			StatementNode col = statement.getFirstChild();
			for (int i = 1; i < columnParts.length; i++) {
				String s = columnParts[i];
				if (s.length() > 0)
					col.getBranches().add(leaf(s.charAt(s.length() - 1) == ',' ? s.substring(0, s.length() - 1) : s,
							Constants.COLUMN_NAME));
			}
		} else {
			for (String column : columnParts) {
				if (column.length() > 0) {
					String s = column;
					if (s.charAt(0) == '(')
						s = s.substring(1, s.length());
					if (s.charAt(s.length() - 1) == ')' || s.charAt(s.length() - 1) == ',')
						s = s.substring(0, s.length() - 1);
					statement.getBranches().add(leaf(s, Constants.COLUMN_NAME));
				}
			}
		}
		return statement;
	}

	private static StatementNode parseFrom(String[] fromParts) {
		StatementNode statement = new StatementNode(Constants.FROM, false);
		for (int i = 0; i < fromParts.length; i++) {
			String s = fromParts[i];
			if (s.charAt(s.length() - 1) == ',')
				s = s.substring(0, s.length() - 1);
			statement.getBranches().add(leaf(s, Constants.TABLE));
		}
		return statement;
	}

	private static StatementNode parseWhere(String[] whereParts) {
		StatementNode statement = new StatementNode(Constants.WHERE, false);
		statement.getBranches().add(parseExpression(whereParts));
		return statement;
	}

	private static StatementNode parseOrderBy(String[] orderParts) {
		StatementNode statement = new StatementNode(Constants.ORDER, false);
		statement.getBranches().add(leaf(orderParts[0], Constants.COLUMN_NAME));
		return statement;
	}

	private static StatementNode parseCreateColumns(String[] createColumnParts) {
		StatementNode statement = new StatementNode(Constants.CREATE_COLUMNS, false);
		for (int i = 0; i < createColumnParts.length / 2; i++) {
			statement.getBranches().add(parseColumnDetails(Arrays.copyOfRange(createColumnParts, 2 * i, 2 * i + 2)));
		}
		return statement;
	}

	private static StatementNode parseValues(String[] valuesParts) throws ParserException {
		if (valuesParts[0].equalsIgnoreCase(Constants.SELECT)) {
			return parseSelect(valuesParts);
		} else {
			StatementNode returnStatement = new StatementNode(Constants.VALUES, false);
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < valuesParts.length; i++) {
				builder.append(valuesParts[i]);
			}
			builder = builder.deleteCharAt(0);
			builder = builder.deleteCharAt(builder.length() - 1);
			String values = builder.toString();
			valuesParts = values.split(",");
			for (String part : valuesParts) {
				String s = trimEnclosingCharacters(part);
				returnStatement.getBranches().add(leaf(s, "VALUE"));
			}
			return returnStatement;
		}
	}

	private static StatementNode parseColumnDetails(String[] columnDetailsParts) {
		StatementNode statement = new StatementNode(Constants.COLUMN_DETAILS, false);
		String columnId = columnDetailsParts[0];
		String type = columnDetailsParts[1];
		columnId = trimEnclosingCharacters(columnId);
		if (type.charAt(type.length() - 1) == ',' || type.charAt(type.length() - 1) == ')')
			type = type.substring(0, type.length() - 1);

		statement.getBranches().add(leaf(columnId, Constants.COLUMN_NAME));
		statement.getBranches().add(leaf(type, Constants.DATATYPE));
		return statement;
	}

	private static StatementNode leaf(String value, String type) {
		StatementNode statement = new StatementNode(type, false);
		statement.getBranches().add(new StatementNode(value, true));
		return statement;
	}

	private static StatementNode parseExpression(String[] tokens) {
		Stack<StatementNode> stack = new Stack<StatementNode>();
		int i = 0;
		while (i < tokens.length) {
			if (operatorPriorityMap.containsKey(tokens[i])) {
				if (stack.size() >= 3) {
					StatementNode last = stack.pop();
					if (operatorPriorityMap.get(tokens[i]) >= operatorPriorityMap.get(stack.peek().getType())) {
						stack.push(last);
						stack.push(new StatementNode(tokens[i], false));
					} else {
						while (stack.size() > 0 && operatorPriorityMap.get(stack.peek().getType()) > operatorPriorityMap
								.get(tokens[i])) {
							StatementNode operator = stack.pop();
							StatementNode anotherOperand = stack.pop();
							operator.getBranches().add(anotherOperand);
							operator.getBranches().add(last);
							last = operator;
						}
						stack.push(last);
						stack.push(new StatementNode(tokens[i], false));
					}
				} else {
					stack.push(new StatementNode(tokens[i], false));
				}
			} else if (HelperFunctions.isInteger(tokens[i])) {
				stack.push(leaf(tokens[i], Constants.INT));
			} else if (tokens[i].charAt(0) == '"') {
				stack.push(leaf(tokens[i].substring(1, tokens[i].length() - 1), Constants.STRING));
			} else if (tokens[i].equals("(")) {
				int count = 0;
				int start = i + 1;
				i += 1;
				while (count != 0 || !tokens[i].equals(")")) {
					if (tokens[i].equals("("))
						count += 1;
					if (tokens[i].equals(")"))
						count -= 1;
					i += 1;
				}
				String[] tokensInClause = Arrays.copyOfRange(tokens, start, i);
				stack.push(parseExpression(tokensInClause));
				i += 1;
				continue;
			} else if (tokens[i].equals("[")) {
				int count = 0;
				int start = i + 1;
				i += 1;
				while (count != 0 || !tokens[i].equals("]")) {
					if (tokens[i].equals("["))
						count += 1;
					if (tokens[i].equals("]"))
						count -= 1;
					i += 1;
				}
				String[] tokensInClause = Arrays.copyOfRange(tokens, start, i);
				stack.push(parseExpression(tokensInClause));
				i += 1;
				continue;
			} else {
				stack.push(leaf(tokens[i], Constants.COLUMN_NAME));
			}
			i++;
		}

		if (stack.size() >= 3) {
			StatementNode operant = stack.pop();
			while (stack.size() >= 2) {
				StatementNode operator = stack.pop();
				operator.getBranches().add(stack.pop());
				operator.getBranches().add(operant);
				operant = operator;
			}
			return operant;
		} else {
			return stack.peek();
		}
	}

	private static String[] parseWhereByOperators(String whereString) {
		List<String> output1 = new ArrayList<>();
		List<String> output2 = new ArrayList<>();
		String[] temp = null;
		// OR
		if (whereString.contains(Constants.OR)) {
			temp = whereString.split(Constants.OR);
			for (int i = 0; i < temp.length; i++) {
				output2.add(temp[i]);
				if (i != temp.length - 1)
					output2.add(Constants.OR);
			}
		}
		// AND
		if (!output2.isEmpty()) {
			for (String s : output2) {
				if (s.contains(Constants.AND)) {
					temp = s.split(Constants.AND);
					for (int i = 0; i < temp.length; i++) {
						output1.add(temp[i]);
						if (i != temp.length - 1)
							output1.add(Constants.AND);
					}
				} else
					output1.add(s);
			}
		} else if (whereString.contains(Constants.AND)) {
			temp = whereString.split(Constants.AND);
			for (int i = 0; i < temp.length; i++) {
				output1.add(temp[i]);
				if (i != temp.length - 1)
					output1.add(Constants.AND);
			}
		}

		// EQUAL
		if (!output1.isEmpty()) {
			output2.clear();
			for (String s : output1) {
				if (s.contains(Constants.EQUAL)) {
					temp = s.split(Constants.EQUAL);
					for (int i = 0; i < temp.length; i++) {
						output2.add(temp[i]);
						if (i != temp.length - 1)
							output2.add(Constants.EQUAL);
					}
				} else
					output2.add(s);
			}
		} else if (whereString.contains(Constants.EQUAL)) {
			output2.clear();
			temp = whereString.split(Constants.EQUAL);
			for (int i = 0; i < temp.length; i++) {
				output2.add(temp[i]);
				if (i != temp.length - 1)
					output2.add(Constants.EQUAL);
			}
		}
		// LESS_THAN
		if (!output2.isEmpty()) {
			output1.clear();
			for (String s : output2) {
				if (s.contains(Constants.LESS_THAN)) {
					temp = s.split(Constants.LESS_THAN);
					for (int i = 0; i < temp.length; i++) {
						output1.add(temp[i]);
						if (i != temp.length - 1)
							output1.add(Constants.LESS_THAN);
					}
				} else
					output1.add(s);
			}
		} else if (whereString.contains(Constants.LESS_THAN)) {
			output1.clear();
			temp = whereString.split(Constants.LESS_THAN);
			for (int i = 0; i < temp.length; i++) {
				output1.add(temp[i]);
				if (i != temp.length - 1)
					output1.add(Constants.LESS_THAN);
			}
		}

		// GREATER_THAN
		if (!output1.isEmpty()) {
			output2.clear();
			for (String s : output1) {
				if (s.contains(Constants.GREATER_THAN)) {
					temp = s.split(Constants.GREATER_THAN);
					for (int i = 0; i < temp.length; i++) {
						output2.add(temp[i]);
						if (i != temp.length - 1)
							output2.add(Constants.GREATER_THAN);
					}
				} else
					output2.add(s);
			}
		} else if (whereString.contains(Constants.GREATER_THAN)) {
			output2.clear();
			temp = whereString.split(Constants.GREATER_THAN);
			for (int i = 0; i < temp.length; i++) {
				output2.add(temp[i]);
				if (i != temp.length - 1)
					output2.add(Constants.GREATER_THAN);
			}
		}
		// ADDITION
		if (!output2.isEmpty()) {
			output1.clear();
			for (String s : output2) {
				if (s.contains(Constants.ADDITION)) {
					temp = s.split("\\" + Constants.ADDITION);
					for (int i = 0; i < temp.length; i++) {
						output1.add(temp[i]);
						if (i != temp.length - 1)
							output1.add(Constants.ADDITION);
					}
				} else
					output1.add(s);
			}
		} else if (whereString.contains(Constants.ADDITION)) {
			output1.clear();
			temp = whereString.split("\\" + Constants.ADDITION);
			for (int i = 0; i < temp.length; i++) {
				output1.add(temp[i]);
				if (i != temp.length - 1)
					output1.add(Constants.ADDITION);
			}
		}

		// SUBTRACTION
		if (!output1.isEmpty()) {
			output2.clear();
			for (String s : output1) {
				if (s.contains(Constants.SUBTRACTION)) {
					temp = s.split(Constants.SUBTRACTION);
					for (int i = 0; i < temp.length; i++) {
						output2.add(temp[i]);
						if (i != temp.length - 1)
							output2.add(Constants.SUBTRACTION);
					}
				} else
					output2.add(s);
			}
		} else if (whereString.contains(Constants.SUBTRACTION)) {
			output2.clear();
			temp = whereString.split(Constants.SUBTRACTION);
			for (int i = 0; i < temp.length; i++) {
				output2.add(temp[i]);
				if (i != temp.length - 1)
					output2.add(Constants.SUBTRACTION);
			}
		}
		// MULTIPLICATION
		if (!output2.isEmpty()) {
			output1.clear();
			for (String s : output2) {
				if (s.contains(Constants.MULTIPLICATION)) {
					temp = s.split("\\" + Constants.MULTIPLICATION);
					for (int i = 0; i < temp.length; i++) {
						output1.add(temp[i]);
						if (i != temp.length - 1)
							output1.add(Constants.MULTIPLICATION);
					}
				} else
					output1.add(s);
			}
		} else if (whereString.contains(Constants.MULTIPLICATION)) {
			output1.clear();
			temp = whereString.split("\\" + Constants.MULTIPLICATION);
			for (int i = 0; i < temp.length; i++) {
				output1.add(temp[i]);
				if (i != temp.length - 1)
					output1.add(Constants.MULTIPLICATION);
			}
		}

		output2.clear();
		for (String s : output1) {
			if (s.charAt(0) == '(')
				s = s.substring(1);
			if (s.charAt(s.length() - 1) == ')')
				s = s.substring(0, s.length() - 1);
			output2.add(s);
		}
		return output2.toArray(new String[0]);
	}

	public static String trimEnclosingCharacters(String string) {
		String res = string;
		if (res.length() == 0)
			return null;
		if (res.charAt(0) == '(')
			res = res.substring(1);
		if (res.charAt(0) == '\"')
			res = res.substring(1);
		if (res.charAt(res.length() - 1) == ')')
			res = res.substring(0, res.length() - 1);
		if (res.charAt(res.length() - 1) == ',')
			res = res.substring(0, res.length() - 1);
		if (res.charAt(res.length() - 1) == '\"')
			res = res.substring(0, res.length() - 1);
		return res;
	}
}
