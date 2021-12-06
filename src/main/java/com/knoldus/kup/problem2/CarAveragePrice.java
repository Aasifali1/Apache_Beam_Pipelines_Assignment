package com.knoldus.kup.problem2;

import org.apache.beam.sdk.Pipeline;
        import org.apache.beam.sdk.io.TextIO;
        import org.apache.beam.sdk.options.Default;
        import org.apache.beam.sdk.options.Description;
        import org.apache.beam.sdk.options.PipelineOptions;
        import org.apache.beam.sdk.options.PipelineOptionsFactory;
        import org.apache.beam.sdk.transforms.*;
        import org.apache.beam.sdk.values.KV;
        import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * This [[ApacheBeamTextIO]] represents a beam pipeline that reads car data(csv file)
 * and compute the average price of different Cars and result is writing to
 * another csv file with header [car,avg_price].
 */

public class CarAveragePrice {

    private static final String CSV_HEADER = "car,price";

    public static void main(String[] args) {

//      Start by defining the options for the pipeline.
        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

//      Then create the pipeline.
        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        /** Create and apply the PCollection 'lines' by applying a 'Read' transform.
         *  Reading Input data
         * */
        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))

//        Apply a MapElements with an anonymous lambda function.
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0], Double.parseDouble(tokens[1]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(carCount -> carCount.getKey() + "," + carCount.getValue()))

//              Writing output data to car_avg_price.csv file
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("car,avg_price"));

//      Run the pipeline
        pipeline.run();
        System.out.println("pipeline executed successfully");
    }


//    Creating our own custom options
    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/car_ad.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/car_avg_price")
        String getOutputFile();

        void setOutputFile(String value);
    }
}