package main;

import parser.StatementNode;
import storageManager.FieldType;
import storageManager.Tuple;

/**
 * Class to check whether the tuple matches with where condition
 */
public class ExpressionEvaluator {

	/** Function to evaluate boolean operator */
	public static boolean evaluateLogicalOperator(StatementNode statement, Tuple tuple) {
		if (statement.getType().equals(Constants.WHERE))
			return evaluateLogicalOperator(statement.getFirstChild(), tuple);

		StatementNode firstChild = statement.getFirstChild();
		StatementNode secondChild = statement.getBranches().get(1);
		switch (statement.getType()) {

		case Constants.OR:
			return evaluateLogicalOperator(firstChild, tuple) || evaluateLogicalOperator(secondChild, tuple);

		case Constants.AND:
			return evaluateLogicalOperator(firstChild, tuple) && evaluateLogicalOperator(secondChild, tuple);

		case Constants.NOT:
			return !(evaluateLogicalOperator(firstChild, tuple));

		case Constants.LESS_THAN:
			return evaluateArithmeticOperator(firstChild, tuple) < evaluateArithmeticOperator(secondChild, tuple);

		case Constants.GREATER_THAN:
			return evaluateArithmeticOperator(firstChild, tuple) > evaluateArithmeticOperator(secondChild, tuple);

		case Constants.EQUAL:
			return evaluateEqualityOperator(firstChild, secondChild, tuple);

		default:
			System.out.println("Unknown operator. Exiting!!!");
			System.exit(0);
		}
		return false;
	}

	public static int evaluateArithmeticOperator(StatementNode statement, Tuple tuple) {
		switch (statement.getType()) {
		case Constants.COLUMN_NAME:
			String column = statement.getFirstChild().getType();
			column = sanitize(column);
			return tuple.getField(column).integer;

		case Constants.INT:
			return Integer.parseInt(statement.getFirstChild().getType());

		case Constants.ADDITION:
			return evaluateArithmeticOperator(statement.getFirstChild(), tuple)
					+ evaluateArithmeticOperator(statement.getBranches().get(1), tuple);

		case Constants.SUBTRACTION:
			return evaluateArithmeticOperator(statement.getFirstChild(), tuple)
					- evaluateArithmeticOperator(statement.getBranches().get(1), tuple);

		case Constants.MULTIPLICATION:
			return evaluateArithmeticOperator(statement.getFirstChild(), tuple)
					* evaluateArithmeticOperator(statement.getBranches().get(1), tuple);

		case Constants.DIVISION:
			return evaluateArithmeticOperator(statement.getFirstChild(), tuple)
					/ evaluateArithmeticOperator(statement.getBranches().get(1), tuple);

		default:
			System.out.println("Wrong type for comparison operator. Exiting!!!");
			System.exit(0);
		}
		return 0;
	}

	private static boolean evaluateEqualityOperator(StatementNode firstOperand, StatementNode secondOperand,
			Tuple tuple) {
		String firstType = null, secondType = null;
		Object firstValue = null, secondValue = null;
		if (firstOperand.getType().equals(Constants.COLUMN_NAME)) {
			String columnName = firstOperand.getFirstChild().getType();
			FieldType type = tuple.getSchema().getFieldType(columnName);
			if (type == FieldType.INT) {
				firstType = Constants.INT;
				firstValue = tuple.getField(columnName).integer;
			} else {
				firstType = Constants.STRING;
				firstValue = tuple.getField(columnName).str;
			}
		} else if (firstOperand.getType().equals(Constants.STRING)) {
			firstType = Constants.STRING;
			firstValue = firstOperand.getFirstChild().getType();
		} else if (firstOperand.getType().equals(Constants.INT)) {
			firstType = Constants.INT;
			firstValue = Integer.parseInt(firstOperand.getFirstChild().getType());
		} else {
			firstType = Constants.INT;
			firstValue = evaluateArithmeticOperator(firstOperand, tuple);
		}

		if (secondOperand.getType().equals(Constants.COLUMN_NAME)) {
			String columnName = secondOperand.getFirstChild().getType();
			FieldType type = tuple.getSchema().getFieldType(columnName);
			if (type == FieldType.INT) {
				secondType = Constants.INT;
				secondValue = tuple.getField(columnName).integer;
			} else {
				secondType = Constants.STRING;
				secondValue = tuple.getField(columnName).str;
			}
		} else if (secondOperand.getType().equals(Constants.STRING)) {
			secondType = Constants.STRING;
			secondValue = secondOperand.getFirstChild().getType();
		} else if (secondOperand.getType().equals(Constants.INT)) {
			secondType = Constants.INT;
			secondValue = Integer.parseInt(secondOperand.getFirstChild().getType());
		} else {
			secondType = Constants.INT;
			secondValue = evaluateArithmeticOperator(firstOperand, tuple);
		}

		if (!firstType.equals(secondType))
			return false;
		else {
			return firstValue.equals(secondValue);
		}
	}

	public static String sanitize(String str) {
		str = str.replace(",", "");
		str = str.replace(";", "");
		str = str.replace("(", "");
		str = str.replace(")", "");
		str = str.replace("\"", "");
		return str;
	}

}
