package com.databricks.spark.csv2;

import java.io.File;
import java.util.HashMap;

import com.databricks.spark.csv2.TestUtils;
import com.databricks.spark.csv2.CsvParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class JavaCsvSuite {
  private transient SQLContext sqlContext;
  private int numCars = 3;

  String carsFile = "src/test/resources/cars.csv";

  private String tempDir = "target/test/csvData/";

  @Before
  public void setUp() {
    sqlContext = new SQLContext(new SparkContext("local[2]", "JavaCsvSuite"));
  }

  @After
  public void tearDown() {
    sqlContext.sparkContext().stop();
    sqlContext = null;
  }

  @Test
  public void testCsvParser() {
    Dataset<Row> df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    int result = df.select("model").collectAsList().size();
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testLoad() {
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("header", "true");
    options.put("path", carsFile);

    Dataset<Row> df = sqlContext.load("com.databricks.spark.csv2", options);
    int result = df.select("year").collectAsList().size();
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testSave() {
    Dataset<Row> df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    TestUtils.deleteRecursively(new File(tempDir));
//    df.select("year", "model").save(tempDir, "com.databricks.spark.csv2");
    df.select("year", "model").write().format("com.databricks.spark.csv2").save(tempDir);

    Dataset<Row> newDf = (new CsvParser()).csvFile(sqlContext, tempDir);
    int result = newDf.select("C1").collectAsList().size();
    Assert.assertEquals(result, numCars);

  }
}
