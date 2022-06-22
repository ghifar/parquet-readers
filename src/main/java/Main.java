import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.connector.expressions.Lit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession sp = SparkSession.builder()
                .appName("oke")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.caseSensitive", "true").getOrCreate();

        Dataset<Row> df = sp.read()
                .parquet("parquet-directory-or-files")
                .coalesce(1);


        /* EXAMPLE
        * Uncomment to try the function */

        // 1. to print schema only
        df.printSchema();

        // 2. example this is to select any columns
        //df.select("`de_sales.settlement_invoice.original_attributes`").show(10, false);

        // 3. export to csv. output folder should in this directory project root
        //toCsv(df.filter("`product.product_source_list.vendor_id`"), "*");

        // 4. to print only contains word in params
        //printContains(df, "de_sales.settlement_invoice.original_attributes");


        // 5. write to parquet file
        //df.withColumn("sales.sales_delivery_item.additional_data.bookingId", df.col("sales.sales_delivery_item.additional_data.bookingId"))
        //        .write()
        //        .mode(SaveMode.Overwrite)
        //        .parquet("parquet-output");


    }

    static void printContains(Dataset<Row> df, String... contains) {
        final String firstFilter = contains[0];
        String secondFilter = " ";
        if (contains.length > 1) {
            secondFilter = contains[1];
        }

        final List<String> columns = Arrays.asList(df.columns());

        String finalSecondFilter = secondFilter;
        final List<String> filteredColumns = columns.stream().filter(s -> s.contains(firstFilter) || s.contains(finalSecondFilter))
                .map(s -> "`" + s + "`")
                .collect(Collectors.toList());

        final List<String> notContain = columns.stream().filter(s -> !s.contains(firstFilter))
                .collect(Collectors.toList());

        df.select("`" + notContain.get(0) + "`", filteredColumns.toArray(new String[0]))
                .drop(notContain.get(0))
                .show(10000, false);
    }

    /**
     * @param df
     * @param column -> * or name of the column
     */
    static void toCsv(Dataset<Row> df, String column) {
        df.select(column)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("csv-output");

    }
}