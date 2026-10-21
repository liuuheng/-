package notes.flink;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
public class PtfExamples {
  public static class SeenCount extends ProcessTableFunction<String> {
    public static class Counter { public long count; }
    public void eval(@StateHint Counter state,
                     @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
      state.count++;
      collect(input.getFieldAs("user_id") + ":" + state.count);
    }
  }
}
