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
		StatementNode firstChild = statement.getBranches().get(0);
		StatementNode secondChild = statement.getBranches().get(1);
		switch (statement.getType()) {
		// If we are at a node one level up
		case Constants.WHERE:
			return evaluateLogicalOperator(firstChild, tuple);

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
		StatementNode firstChild = statement.getBranches().get(0);
		StatementNode secondChild = statement.getBranches().get(1);
		switch (statement.getType()) {
		case Constants.COLUMN_NAME:
			return tuple.getField(firstChild.getType()).integer;

		case Constants.INT:
			return Integer.parseInt(firstChild.getType());

		case Constants.ADDITION:
			return evaluateArithmeticOperator(firstChild, tuple) + evaluateArithmeticOperator(secondChild, tuple);

		case Constants.SUBTRACTION:
			return evaluateArithmeticOperator(firstChild, tuple) - evaluateArithmeticOperator(secondChild, tuple);

		case Constants.MULTIPLICATION:
			return evaluateArithmeticOperator(firstChild, tuple) * evaluateArithmeticOperator(secondChild, tuple);

		case Constants.DIVISION:
			return evaluateArithmeticOperator(firstChild, tuple) / evaluateArithmeticOperator(secondChild, tuple);

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
			String columnName = firstOperand.getBranches().get(0).getType();
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
			firstValue = firstOperand.getBranches().get(0).getType();
		} else if (firstOperand.getType().equals(Constants.INT)) {
			firstType = Constants.INT;
			firstValue = Integer.parseInt(firstOperand.getBranches().get(0).getType());
		}

		if (secondOperand.getType().equals(Constants.COLUMN_NAME)) {
			String columnName = secondOperand.getBranches().get(0).getType();
			FieldType type = tuple.getSchema().getFieldType(columnName);
			if (type == FieldType.INT) {
				firstType = Constants.INT;
				firstValue = tuple.getField(columnName).integer;
			} else {
				firstType = Constants.STRING;
				firstValue = tuple.getField(columnName).str;
			}
		} else if (secondOperand.getType().equals(Constants.STRING)) {
			firstType = Constants.STRING;
			firstValue = secondOperand.getBranches().get(0).getType();
		} else if (secondOperand.getType().equals(Constants.INT)) {
			firstType = Constants.INT;
			firstValue = Integer.parseInt(secondOperand.getBranches().get(0).getType());
		}

		if (!firstType.equals(secondType))
			return false;
		else {
			return firstValue.equals(secondValue);
		}
	}

}
