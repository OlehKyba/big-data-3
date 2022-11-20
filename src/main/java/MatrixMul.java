import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;

class MatrixItem implements Writable {
    public static final int M = 0;
    public static final int N = 1;
    private int type;
    private int index;
    private double value;

    MatrixItem() {
        type = 0;
        index = 0;
        value = 0;
    }
    MatrixItem(int type, int index, double value) {
        this.type = type;
        this.index = index;
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public double getValue() {
        return value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        type = input.readInt();
        index = input.readInt();
        value = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(type);
        output.writeInt(index);
        output.writeDouble(value);
    }
}

class MatrixIndexes implements WritableComparable<MatrixIndexes> {
    private int i;
    private int j;

    MatrixIndexes() {
        i = 0;
        j = 0;
    }

    MatrixIndexes(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }

    @Override
    public int compareTo(MatrixIndexes compare) {
        if (i > compare.i) {
            return 1;
        } else if ( i < compare.i) {
            return -1;
        } else {
            if(j > compare.j) {
                return 1;
            } else if (j < compare.j) {
                return -1;
            }
        }
        return 0;
    }

    public String toString() {
        return i + "," + j + ",";
    }

}

abstract class MatrixMapper extends Mapper<Object, Text, IntWritable, MatrixItem> {
    public abstract int getMatrixIndex();
    public abstract int getMatrixNumber();
    public abstract int getKeyIndex();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");

        int index = Integer.parseInt(row[getMatrixIndex()]);
        double matrixValue = Double.parseDouble(row[2]);

        MatrixItem matrixItem = new MatrixItem(getMatrixNumber(), index, matrixValue);

        IntWritable mapKey = new IntWritable(Integer.parseInt(row[getKeyIndex()]));
        context.write(mapKey, matrixItem);
    }
}

class MatrixMapperM extends MatrixMapper {
    @Override
    public int getMatrixIndex() {
        return 0;
    }

    @Override
    public int getMatrixNumber() {
        return MatrixItem.M;
    }

    @Override
    public int getKeyIndex() {
        return 1;
    }
}

class MatrixMapperN extends MatrixMapper {
    @Override
    public int getMatrixIndex() {
        return 1;
    }

    @Override
    public int getMatrixNumber() {
        return MatrixItem.N;
    }

    @Override
    public int getKeyIndex() {
        return 0;
    }
}

class MulReducer extends Reducer<IntWritable, MatrixItem, MatrixIndexes, DoubleWritable> {
    @Override
    public void reduce(
            IntWritable key, Iterable<MatrixItem> values, Context context
    ) throws IOException, InterruptedException {
        ArrayList<MatrixItem> M = new ArrayList<MatrixItem>();
        ArrayList<MatrixItem> N = new ArrayList<MatrixItem>();

        Configuration config = context.getConfiguration();

        for(MatrixItem matrixItem : values) {
            MatrixItem tempMatrixItem = ReflectionUtils.newInstance(MatrixItem.class, config);
            ReflectionUtils.copy(config, matrixItem, tempMatrixItem);
            int matrixType = tempMatrixItem.getType();
            if (matrixType == MatrixItem.M) {
                M.add(tempMatrixItem);
            } else if(matrixType == MatrixItem.N) {
                N.add(tempMatrixItem);
            }
        }

        for (MatrixItem mMatrixItem : M) {
            for (MatrixItem nMatrixItem : N) {
                MatrixIndexes indexes = new MatrixIndexes(mMatrixItem.getIndex(), nMatrixItem.getIndex());
                double multiplyOutput = mMatrixItem.getValue() * nMatrixItem.getValue();
                context.write(indexes, new DoubleWritable(multiplyOutput));
            }
        }
    }
}

class MulResultsMapper extends Mapper<Object, Text, MatrixIndexes, DoubleWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");

        MatrixIndexes matrixIndexes = new MatrixIndexes(Integer.parseInt(row[0]), Integer.parseInt(row[1]));
        DoubleWritable result = new DoubleWritable(Double.parseDouble(row[2]));

        context.write(matrixIndexes, result);
    }
}

class SumReducer extends Reducer<MatrixIndexes, DoubleWritable, MatrixIndexes, DoubleWritable> {
    @Override
    public void reduce(
            MatrixIndexes key, Iterable<DoubleWritable> values, Context context
    ) throws IOException, InterruptedException {
        double sum = 0.0;
        for(DoubleWritable value : values) {
            sum += value.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}

public class MatrixMul {
    public static void main(String[] args) throws Exception {
        Path mMatrixPath = new Path(args[0]);
        Path nMatrixPath = new Path(args[1]);
        Path intermediateDirPath = new Path(args[2]);
        Path outputDirPath = new Path(args[3]);

        Job intermediateJob = Job.getInstance();
        intermediateJob.setJobName("IntermediateJob");
        intermediateJob.setJarByClass(MatrixMul.class);

        MultipleInputs.addInputPath(intermediateJob, mMatrixPath, TextInputFormat.class, MatrixMapperM.class);
        MultipleInputs.addInputPath(intermediateJob, nMatrixPath, TextInputFormat.class, MatrixMapperN.class);
        intermediateJob.setReducerClass(MulReducer.class);
        intermediateJob.setMapOutputKeyClass(IntWritable.class);
        intermediateJob.setMapOutputValueClass(MatrixItem.class);
        intermediateJob.setOutputKeyClass(MatrixIndexes.class);
        intermediateJob.setOutputValueClass(DoubleWritable.class);
        intermediateJob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(intermediateJob, intermediateDirPath);

        intermediateJob.waitForCompletion(true);

        Job finalJob = Job.getInstance();
        finalJob.setJobName("FinalJob");
        finalJob.setJarByClass(MatrixMul.class);
        finalJob.setMapperClass(MulResultsMapper.class);
        finalJob.setReducerClass(SumReducer.class);
        finalJob.setMapOutputKeyClass(MatrixIndexes.class);
        finalJob.setMapOutputValueClass(DoubleWritable.class);
        finalJob.setOutputKeyClass(MatrixIndexes.class);
        finalJob.setOutputValueClass(DoubleWritable.class);
        finalJob.setInputFormatClass(TextInputFormat.class);
        finalJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(finalJob, intermediateDirPath);
        FileOutputFormat.setOutputPath(finalJob, outputDirPath);

        finalJob.waitForCompletion(true);
    }
}