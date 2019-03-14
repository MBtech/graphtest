package com.adventuresincs.examples;

/**
 * Created by zainababbas on 18/04/2017.
 */

import com.adventuresincs.examples.utils.CustomKeySelector;
import com.adventuresincs.examples.utils.ImbalancedPartitioner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.NullValue;

public class GSASSSP {

    public static boolean fileOutput = false;
    public static Long srcVertexId = 1l;
    public static String edgesInputPath = "testgraph.txt";
    public static String outputPath = "";
    public static int maxIterations = 5;
    public static int numPartitions = 3;
    public static String pStrategy = "hash";

    public static void main( String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(numPartitions);

        DataSet<Edge<Long, NullValue>> data = env.readTextFile(edgesInputPath).setParallelism(1).map(new MapFunction<String, Edge<Long, NullValue>>() {

            @Override
            public Edge<Long, NullValue> map(String s) {
                String[] fields = s.split("\\t");
                long src = Long.parseLong(fields[0]);
                long trg = Long.parseLong(fields[1]);
                return new Edge<>(src, trg, NullValue.getInstance());
            }
        }).setParallelism(1);

        Partitioner partitioner = new ImbalancedPartitioner<>();

        Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data.partitionCustom(partitioner, new CustomKeySelector<>(0)), new InitVertices(srcVertexId), env);

        // Execute the GSA iteration
        Graph<Long, Double, NullValue> result = graph.runGatherSumApplyIteration(
                new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);

        // Extract the vertices as the result
        DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();



        if (fileOutput) {
            singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
        } else {
            singleSourceShortestPaths.print();
        }
//        JobExecutionResult result1 = env.execute("My Flink Job1");

//        System.out.println("The job1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");//appends the string to the file
//        System.out.println("The job1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute");
    }

    // --------------------------------------------------------------------------------------------
    //  Single Source Shortest Path UDFs
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static final class InitVertices implements MapFunction<Long, Double> {

        private long srcId;

        public InitVertices(long srcId) {
            this.srcId = srcId;
        }

        public Double map(Long id) {
            if (id.equals(srcId)) {
                return 0.0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class CalculateDistances extends GatherFunction<Double, NullValue, Double> {

        public Double gather(Neighbor<Double, NullValue> neighbor) {
            return neighbor.getNeighborValue() + 1;
        }

    }

    @SuppressWarnings("serial")
    private static final class ChooseMinDistance extends SumFunction<Double, NullValue, Double> {

        public Double sum(Double newValue, Double currentValue) {
            return Math.min(newValue, currentValue);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateDistance extends ApplyFunction<Long, Double, Double> {

        public void apply(Double newDistance, Double oldDistance) {
            if (newDistance < oldDistance) {
                setResult(newDistance);
            }
        }

    }

    public static boolean parseParameters(String[] args) {

        if (args.length > 1) {
            if (args.length != 7) {
                System.err.println("Usage: <source vertex id> <input edges path> " +
                        "<output path> <num iterations> <no. of partitions> <partitioning algorithm>");
                return false;
            }

            fileOutput = true;
            srcVertexId = Long.parseLong(args[0]);
            edgesInputPath = args[1];
            outputPath = args[2];
            maxIterations = Integer.parseInt(args[3]);
            numPartitions = Integer.parseInt(args[4]);
            pStrategy = args[5];
        }else if (args.length > 0){
            edgesInputPath = args[0];
        }else{
            System.out.println("Using default arguments");
            System.out.println("Usage: <source vertex id> <input edges path> " +
                    "<output path> <num iterations> <no. of partitions> <partitioning algorithm>");
        }
        return true;
    }


}

