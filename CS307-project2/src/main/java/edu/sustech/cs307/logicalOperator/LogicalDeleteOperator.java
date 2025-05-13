package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

public class LogicalDeleteOperator extends LogicalOperator {

    private final String tableName;
    private final Expression whereExpr;  // 可为 null，表示无 WHERE 条件
    private final LogicalOperator child;

    public LogicalDeleteOperator(LogicalOperator child, String tableName, Expression whereExpr) {
        super(Collections.singletonList(child));  // Delete 通常只对一个表操作
        this.child = child;
        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public LogicalOperator getChild() {
        return child;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LogicalDeleteOperator(table=")
                .append(tableName)
                .append(", where=")
                .append(whereExpr)
                .append(")");

        if (child != null) {
            String[] childLines = child.toString().split("\\R");
            sb.append("\n└── ").append(childLines[0]);
            for (int i = 1; i < childLines.length; i++) {
                sb.append("\n    ").append(childLines[i]);
            }
        }

        return sb.toString();
    }
}
