package parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import main.Constants;

public class Parser {

	private static Map<String, Integer> operatorPriorityMap;

	static {
		operatorPriorityMap = new HashMap<String, Integer>();
		operatorPriorityMap.put("OR", 0);
		operatorPriorityMap.put("AND", 1);
		operatorPriorityMap.put("NOT", 2);
		operatorPriorityMap.put("<", 3);
		operatorPriorityMap.put(">", 3);
		operatorPriorityMap.put("=", 3);
		operatorPriorityMap.put("+", 4);
		operatorPriorityMap.put("-", 4);
		operatorPriorityMap.put("*", 5);
		operatorPriorityMap.put("/", 5);
	}

	public static StatementNode startParse(String query) throws ParserException {
		System.out.println("Parsing query: " + query);
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
			statement.getBranches().add(leaf(parts[2], Constants.RELATION));
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
			StatementNode whereStatement = parseWhere(Arrays.copyOfRange(parts, whereIndex + 1, end));
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
			StatementNode orderStatement = parseOrder(Arrays.copyOfRange(parts, orderByIndex + 2, parts.length));
			statement.getBranches().add(orderStatement);
		}
		return statement;
	}

	private static StatementNode parseCreate(String[] parts) throws ParserException {
		if (!parts[1].equalsIgnoreCase("TABLE"))
			throw new ParserException("TABLE keyword wrong in create statement.");
		StatementNode statement = new StatementNode(Constants.CREATE, false);
		statement.getBranches().add(leaf(parts[2], Constants.RELATION));
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
			returnStatement.getBranches().add(leaf(parts[2], Constants.RELATION));
			returnStatement.getBranches().add(parseColumns(Arrays.copyOfRange(parts, 3, valuesIndex)));
			returnStatement.getBranches().add(parseValues(Arrays.copyOfRange(parts, valuesIndex + 1, parts.length)));
			statement = returnStatement;
		}

		if (selectIndex > 0) {
			StatementNode returnStatement = new StatementNode(Constants.INSERT, false);
			returnStatement.getBranches().add(leaf(parts[2], Constants.RELATION));
			returnStatement.getBranches().add(parseColumns(Arrays.copyOfRange(parts, 3, selectIndex)));
			returnStatement.getBranches().add(parseValues(Arrays.copyOfRange(parts, selectIndex, parts.length)));
			statement = returnStatement;
		}
		return statement;
	}

	private static StatementNode parseDelete(String[] parts) throws ParserException {
		if (!parts[1].equalsIgnoreCase("FROM"))
			throw new ParserException("FROM keyword wrong in delete statement.");
		StatementNode statement = new StatementNode(Constants.DELETE, false);
		statement.getBranches().add(leaf(parts[2], Constants.RELATION));
		if (parts.length > 3 && parts[3].equalsIgnoreCase(Constants.WHERE)) {
			statement.getBranches().add(parseWhere(Arrays.copyOfRange(parts, 4, parts.length)));
		}
		return statement;
	}

	private static StatementNode parseColumns(String[] columnParts) {
		StatementNode statement = new StatementNode(Constants.COLUMNS, false);
		if (columnParts[0].equalsIgnoreCase(Constants.DISTINCT)) {
			statement.getBranches().add(new StatementNode(Constants.DISTINCT, false));
			StatementNode col = statement.getBranches().get(0);
			for (int i = 1; i < columnParts.length; i++) {
				String s = columnParts[i];
				if (s.length() > 0)
					col.getBranches().add(leaf(s.charAt(s.length() - 1) == ',' ? s.substring(0, s.length() - 1) : s,
							Constants.COLUMN_ID));
			}
		} else {
			for (String column : columnParts) {
				if (column.length() > 0) {
					String s = column;
					if (s.charAt(0) == '(')
						s = s.substring(1, s.length());
					if (s.charAt(s.length() - 1) == ')' || s.charAt(s.length() - 1) == ',')
						s = s.substring(0, s.length() - 1);
					statement.getBranches().add(leaf(s, Constants.COLUMN_ID));
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
			statement.getBranches().add(leaf(s, Constants.RELATION));
		}
		return statement;
	}

	private static StatementNode parseWhere(String[] whereParts) {
		StatementNode statement = new StatementNode(Constants.EXPRESSION, false);
		statement.getBranches().add(evaluateExpression(whereParts));
		return statement;
	}

	private static StatementNode parseOrder(String[] orderParts) {
		StatementNode statement = new StatementNode(Constants.ORDER, false);
		statement.getBranches().add(leaf(orderParts[0], Constants.COLUMN_ID));
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
		// if (columnId.charAt(0) == '(')
		// columnId = columnId.substring(1);
		// if (columnId.charAt(columnId.length() - 1) == ',' ||
		// columnId.charAt(columnId.length() - 1) == ')')
		// columnId = columnId.substring(0, columnId.length() - 1);
		if (type.charAt(type.length() - 1) == ',' || type.charAt(type.length() - 1) == ')')
			type = type.substring(0, type.length() - 1);

		statement.getBranches().add(leaf(columnId, Constants.COLUMN_ID));
		statement.getBranches().add(leaf(type, Constants.DATATYPE));
		return statement;
	}

	private static StatementNode leaf(String string, String type) {
		if (type.equals(Constants.COLUMN_ID)) {
			StatementNode statement = new StatementNode(Constants.COLUMN_ID, false);
			String[] id = string.split("\\.");
			for (String t : id) {
				statement.getBranches().add(new StatementNode(t, true));
			}
			return statement;
		} else if (type.equals(Constants.RELATION)) {
			StatementNode statement = new StatementNode(Constants.RELATION, false);
			statement.getBranches().add(new StatementNode(string, true));
			return statement;
		} else {
			StatementNode statement = new StatementNode(type, false);
			statement.getBranches().add(new StatementNode(string, true));
			return statement;
		}
	}

	private static StatementNode evaluateExpression(String[] tokens) {
		Stack<StatementNode> stack = new Stack<StatementNode>();
		int i = 0;
		while (i < tokens.length) {
			if (tokens[i].equals("NOT")) {
				stack.push(new StatementNode(tokens[i], false));
			} else if (operatorPriorityMap.containsKey(tokens[i])) {
				if (stack.size() >= 3) {
					StatementNode last = stack.pop();
					if (operatorPriorityMap.get(tokens[i]) >= operatorPriorityMap.get(stack.peek().getType())) {
						stack.push(last);
						stack.push(new StatementNode(tokens[i], false));
					} else {
						while (stack.size() > 0 && operatorPriorityMap.get(stack.peek().getType()) > operatorPriorityMap
								.get(tokens[i])) {
							StatementNode operator = stack.pop();
							if (operator.getType().equals("NOT")) {
								operator.getBranches().add(last);
								last = operator;
								continue;
							}
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
			} else if (isInteger(tokens[i])) {
				stack.push(leaf(tokens[i], "INT"));
			} else if (tokens[i].charAt(0) == '"') {
				stack.push(leaf(tokens[i].substring(1, tokens[i].length() - 1), "STRING"));
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
				stack.push(evaluateExpression(tokensInClause));
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
				stack.push(evaluateExpression(tokensInClause));
				i += 1;
				continue;
			} else {
				stack.push(leaf(tokens[i], Constants.COLUMN_ID));
			}
			i++;
		}

		if (stack.size() >= 3) {
			StatementNode operant = stack.pop();
			while (stack.size() >= 2) {
				StatementNode operator = stack.pop();
				if (operator.getType().equals("NOT")) {
					operator.getBranches().add(operant);
					operant = operator;
					continue;
				}
				operator.getBranches().add(stack.pop());
				operator.getBranches().add(operant);
				operant = operator;
			}
			return operant;
		} else {
			return stack.peek();
		}
	}

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException err) {
			return false;
		}
	}

	public static String trimEnclosingCharacters(String string) {
		String res = string;
		if (res.length() == 0)
			return null;
		if (res.charAt(0) == '(')
			res = res.substring(1);
		if (res.charAt(0) == '"')
			res = res.substring(1);
		if (res.charAt(res.length() - 1) == ')')
			res = res.substring(0, res.length() - 1);
		if (res.charAt(res.length() - 1) == ',')
			res = res.substring(0, res.length() - 1);
		if (res.charAt(res.length() - 1) == '"')
			res = res.substring(0, res.length() - 1);
		return res;
	}
}
