package hackathon

import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import scala.collection.immutable.TreeMap




object Articles {
  def main(args: Array[String]){
    val nargs = 4
    if(args.length < nargs){
      System.err.println(
        s"""
           |Usage: RECOMMENDATION ENGINE <exec_mode> <sku_id><input_dataset_location><output_dataset_location>
         """.stripMargin)
      System.exit(1)
    }
    val Array(execMode,skuId,inputPath,outputPath) = args
    println("Execution Mode: %s".format(execMode))
    println("SKU ID: %s".format(skuId))
    println("Input DataSet Location: %s".format(inputPath))
    println("Output/Result DataSet Location: %s".format(outputPath))

    var write = new FileWriter(outputPath)
    var bw = new BufferedWriter(write)

    def appName="Articles-Recommendation-Engine"

    val conf=new SparkConf()
      .setAppName(appName)
      .setMaster(execMode)
    val sc=new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)

    //JSON Schema Definition/Creation
    val innerStruct = StructType(
      StructField("att-a",StringType,true) ::
        StructField("att-b",StringType,true) ::
        StructField("att-c",StringType,true) ::
        StructField("att-d",StringType,true) ::
        StructField("att-e",StringType,true) ::
        StructField("att-f",StringType,true) ::
        StructField("att-g",StringType,true) ::
        StructField("att-h",StringType,true) ::
        StructField("att-i",StringType,true) ::
        StructField("att-j",StringType,true) ::
        Nil
    )
    val schema1 = StructType(
      StructField("sku",StringType,true) ::
        StructField("attributes",innerStruct,true) ::
        Nil
    )

    // Loading the data from JSON file into DataFrame
    val fullDataSet=sqlContext.read.schema(schema1)
      .json(inputPath)
      .select("sku","attributes.att-a","attributes.att-b","attributes.att-c","attributes.att-d","attributes.att-e","attributes.att-f","attributes.att-g","attributes.att-h","attributes.att-i","attributes.att-j")
    fullDataSet.cache()

    // Creating a temporary table using DataFrame
    fullDataSet.registerTempTable("fullDataSetTable")
    var list3=List[Any]()
    val desiredSku=sqlContext.sql("select a.* from fullDataSetTable a where sku = '"+skuId+"'")

    // Looping records over one by one and calculating the similar SKU's

    desiredSku.rdd.collect().map(row => {
      var outerSku = row(0)
      var oAtt1 = row(1)
      var oAtt2 = row(2)
      var oAtt3 = row(3)
      var oAtt4 = row(4)
      var oAtt5 = row(5)
      var oAtt6 = row(6)
      var oAtt7 = row(7)
      var oAtt8 = row(8)
      var oAtt9 = row(9)
      var oAtt10 = row(10)
      var max = 0
      var abc = " "
      var treeMap1 = TreeMap.empty[Int,List[Int]]
      var treeMap2 = TreeMap.empty[Int,List[Any]]
      fullDataSet.rdd.collect().map(row => {
        var i = 0
        var jList = List[Int]()
        var attrList = List[Any]()
        var innerSku = row(0)
        var iAtt1 = row(1)
        var iAtt2 = row(2)
        var iAtt3 = row(3)
        var iAtt4 = row(4)
        var iAtt5 = row(5)
        var iAtt6 = row(6)
        var iAtt7 = row(7)
        var iAtt8 = row(8)
        var iAtt9 = row(9)
        var iAtt10 = row(10)

        if(!(outerSku.equals(innerSku))) {
          if (oAtt1.equals(iAtt1)) {
            i += 1; jList = 1 :: jList
          };

          if (oAtt2.equals(iAtt2)) {
            i += 1; jList = 2 :: jList
          };

          if (oAtt3.equals(iAtt3)) {
            i += 1; jList = 3 :: jList
          };

          if (oAtt4.equals(iAtt4)) {
            i += 1; jList = 4 :: jList
          };

          if (oAtt5.equals(iAtt5)) {
            i += 1; jList = 5 :: jList
          };

          if (oAtt6.equals(iAtt6)) {
            i += 1; jList = 6 :: jList
          };

          if (oAtt7.equals(iAtt7)) {
            i += 1; jList = 7 :: jList
          };

          if (oAtt8.equals(iAtt8)) {
            i += 1; jList = 8 :: jList
          };

          if (oAtt9.equals(iAtt9)) {
            i += 1; jList = 9 :: jList
          };

          if (oAtt10.equals(iAtt10)) {
            i += 1; jList = 10 :: jList
          };

          if (i > max) {
            max = i
            treeMap1 += (i -> jList)
            attrList = row :: attrList
            treeMap2 += (i -> attrList)
          } else {
            var list2 = List[Int]()
            if (treeMap1.contains(i)) {
              list2 = treeMap1.apply(i)
              var newList = jList.filterNot(list2.contains(_)).sorted
              var oldList = list2.filterNot(jList.contains(_)).sorted
              var oldSize = oldList.size
              var newSize = newList.size
              if(oldSize == 0){

              }
              else {
                var a = newList(0)
                var b = oldList(0)
                if (a < b) {
                  treeMap1 += (i -> jList)
                  attrList = row :: attrList
                  treeMap2 += (i -> attrList)
                }
              }

            } else {
              treeMap1 += (i -> jList)
              attrList = row :: attrList
              treeMap2 += (i -> attrList)
            }

          }
        }

      }
      )

      val valueSet = treeMap2.values
      abc = treeMap2.values.mkString("|")
      var res = abc.split("""\|""")
      write.write("Similar articles for the given SKU ID: " +outerSku)
      write.write("\n")
      for (iter <- (res.length-1) to 0 by -1){
        write.write(res(iter))
        write.write("\n")
      }
    }
    )
    write.close()
  }

}
