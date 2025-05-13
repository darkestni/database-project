package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class DeleteOperator implements PhysicalOperator {
    private final SeqScanOperator seqScanOperator;
    private final String tableName;
    private final Expression whereExpr;

    private int deleteCount;
    private boolean isDone;

    public DeleteOperator(PhysicalOperator inputOperator, String tableName, Expression whereExpr) {
        if (!(inputOperator instanceof SeqScanOperator seqScan)) {
            throw new RuntimeException("DeleteOperator only accepts SeqScanOperator as input");
        }
        this.seqScanOperator = seqScan;
        this.tableName = tableName;
        this.whereExpr = whereExpr;
        this.deleteCount = 0;
        this.isDone = false;
    }

    @Override
    public boolean hasNext() {
        return !isDone;
    }

    @Override
    public void Begin() throws DBException {
        seqScanOperator.Begin();
        RecordFileHandle fileHandle = seqScanOperator.getFileHandle();

        while (seqScanOperator.hasNext()) {
            seqScanOperator.Next();
            TableTuple tuple = (TableTuple) seqScanOperator.Current();

            if (whereExpr == null || tuple.eval_expr(whereExpr)) {
                fileHandle.DeleteRecord(tuple.getRID());
                deleteCount++;
            }
        }
    }

    @Override
    public void Next() {
        isDone = true;
    }

    @Override
    public Tuple Current() {
        if (isDone) {
            ArrayList<Value> result = new ArrayList<>();
            result.add(new Value(deleteCount, ValueType.INTEGER));
            return new TempTuple(result);
        } else {
            throw new RuntimeException("Call Next() before Current()");
        }
    }

    @Override
    public void Close() {
        seqScanOperator.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.add(new ColumnMeta("delete", "numberOfDeletedRows", ValueType.INTEGER, 0, 0));
        return schema;
    }

    public void reset() {
        deleteCount = 0;
        isDone = false;
    }

    public Tuple getNextTuple() {
        if (hasNext()) {
            Next();
            return Current();
        }
        return null;
    }

    public String getTableName() {
        return tableName;
    }
}
