package com.spark.ubstest

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object EODPosCalc {

  def main(args: Array[String]): Unit = {

    try {
      // Scehma to create type safe Dataset
      val v_schema = new StructType()
        .add("Instrument", StringType, true)
        .add("Account", IntegerType, true)
        .add("AccountType", StringType, true)
        .add("Quantity", IntegerType, true)
      val v_EodSchema = new StructType()
        .add("Instrument", StringType, true)
        .add("Account", IntegerType, true)
        .add("AccountType", StringType, true)
        .add("Quantity", IntegerType, true)
        .add("Delta", IntegerType, true)

      Logger.getLogger("org").setLevel(Level.ERROR) // Set Log level to error

      // Initialize Spark configuration Variable
      val v_session = SparkSession
        .builder()
        .appName("EODPosCalc")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .master("local[1]")
        .getOrCreate()
      val v_DataSetReader = v_session.read // DataSet Reader Object
      val v_sc = v_session.sparkContext
      v_sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") // Suppress Success File in output
      val fs = FileSystem.get(v_session.sparkContext.hadoopConfiguration)

      // Read the Input Transaction JSON file
      val v_TxnDS = v_DataSetReader.option("multiLine", true).json("in/Input_Transactions.txt")

      // Consolidate All the Trasaction
      val v_ConsolidtdTxnDS = v_TxnDS
        .groupBy("Instrument", "TransactionType")
        .sum()
        .sort("Instrument", "TransactionType")
        .persist(StorageLevel.MEMORY_ONLY)

      // Read Input Start Position File
      val v_StartPosDS = v_DataSetReader
        .option("header", "true")
        .option("inferSchema", value = false)
        .schema(v_schema)
        .csv("in/Input_StartOfDay_Positions.txt")
        .persist(StorageLevel.MEMORY_ONLY)

      // Get Distinct Values for Instrument to start loop
      val v_DistinctStartPosDS = v_StartPosDS.select("Instrument").distinct().collect().map(x => x(0).toString)

      for (v_inst <- v_DistinctStartPosDS.indices) {

        var v_TxnType: String = ""
        var v_EodPosEQty: Long = 0
        var v_EodPosIQty: Long = 0
        var v_DeltaQtyBS: Long = 0
        var v_BTypeTxQty: Int = 0
        var v_STypeTxQty: Int = 0
        val v_InstrumentValue: String = v_DistinctStartPosDS(v_inst).toString

        // Start retrieving Position for E Type Account
        val v_ETypeQtyArry = v_StartPosDS
          .filter(v_StartPosDS.col("Instrument") === v_InstrumentValue && v_StartPosDS.col("AccountType") === "E")
          .select("Account", "AccountType", "Quantity")
          .collect()

        val v_StPosEQty: Long = v_ETypeQtyArry(0)(2).toString.toLong // Start position of E Type Account
        val v_EAccNo: Int = v_ETypeQtyArry(0)(0).toString.toInt // Account Number of E Type Account
        // End retrieving Position for E Type Account

        // End retrieving Position for i Type Account
        val v_ITypeQtyArry = v_StartPosDS
          .filter(v_StartPosDS.col("Instrument") === v_InstrumentValue && v_StartPosDS.col("AccountType") === "I")
          .select("Account", "AccountType", "Quantity")
          .collect()

        val v_StPosIQty: Long = v_ITypeQtyArry(0)(2).toString.toLong // Start position of I Type Account
        val v_IAccNo: Int = v_ITypeQtyArry(0)(0).toString.toInt // Account Number of I Type Account
        // End retrieving Position for i Type Account

        // Start Apply Exception Handelling for NO Buy transaction
        try {
          val v_BTypeTxArry = v_ConsolidtdTxnDS
            .filter(v_ConsolidtdTxnDS.col("Instrument") === v_InstrumentValue && v_ConsolidtdTxnDS.col("TransactionType") === "B")
            .select("sum(TransactionQuantity)")
            .collect()
            .map(x => x(0).toString)

          v_BTypeTxQty = v_BTypeTxArry(0).toInt // B Type consolidated Transaction Qty
        }
        catch {
          case e: Exception => v_BTypeTxQty = 0
        } // End Apply Exception Handelling for NO Buy transaction
        // Start Apply Exception Handelling for NO Buy transaction
        try {
          val v_STypeTxArry = v_ConsolidtdTxnDS
            .filter(v_ConsolidtdTxnDS.col("Instrument") === v_InstrumentValue && v_ConsolidtdTxnDS.col("TransactionType") === "S")
            .select("sum(TransactionQuantity)")
            .collect()
            .map(x => x(0).toString)

          v_STypeTxQty = v_STypeTxArry(0).toInt // S Type consolidated Transaction Qty
        }
        catch {
          case e: Exception => v_STypeTxQty = 0
        } // End Apply Exception Handelling for NO Buy transaction

        // Find out the Transaction type to apply Debit/Credit calculation on Quantity
        if (v_STypeTxQty >= v_BTypeTxQty) {
          v_TxnType = "S"
        }
        else {
          v_TxnType = "B"
        }

        v_TxnType match {

          case "B" => v_DeltaQtyBS = v_BTypeTxQty - v_STypeTxQty
            v_EodPosEQty = v_StPosEQty + v_DeltaQtyBS
            v_EodPosIQty = v_StPosIQty - v_DeltaQtyBS

          case "S" => v_DeltaQtyBS = v_STypeTxQty - v_BTypeTxQty
            v_EodPosEQty = v_StPosEQty - v_DeltaQtyBS
            v_EodPosIQty = v_StPosIQty + v_DeltaQtyBS
        } // End Match Case

        val v_DeltaPosE = v_EodPosEQty - v_StPosEQty // Delta Quantity for E type Account
        val v_DeltaPosI = v_EodPosIQty - v_StPosIQty // Delta Quantity for I type Account

        //  Create Dataset to write desired op to Temp Location
        import v_session.implicits._
        val EODPosSeq = Seq(Response(v_InstrumentValue, v_EAccNo, "E", v_EodPosEQty, v_DeltaPosE), Response(v_InstrumentValue, v_IAccNo, "I", v_EodPosIQty, v_DeltaPosI))
        val v_TempEODPosDS = v_session.createDataset(EODPosSeq)

        v_TempEODPosDS.write.mode("Append").option("header", "true").csv("temp")

      } // End For Loop

      // Unpersist the Datasets
      v_StartPosDS.unpersist()
      v_ConsolidtdTxnDS.unpersist()

      val v_FinalEodPosDS = v_DataSetReader
        .option("inferSchema", value = false)
        .schema(v_EodSchema)
        .option("header", "true")
        .csv("temp")
        .persist(StorageLevel.MEMORY_ONLY)

      val v_MinMaxNetVol = v_FinalEodPosDS.orderBy(v_FinalEodPosDS.col("Delta").desc)

      // Max & Min Value File First record is with MAX NET VOLUME and last is MIN NET VOLUME
      v_MinMaxNetVol.coalesce(1).write.mode("Overwrite").option("header", "true").csv("out/Net_Voume_of_Day")

      //  Final Output END OF DAY POSITIONS
      v_FinalEodPosDS.write.mode("Overwrite").option("header", "true").csv("out/Cal_EndOfDay_Positions")

      v_FinalEodPosDS.unpersist()

      // Remove any Temp files
      if (fs.exists(new Path("temp")))
        fs.delete(new Path("temp"), true)

      v_session.stop() // Stop Spark Session

    } // End Try Block
    catch {
      case e: Exception => {
        println("################## Exception in the Process ##################")
        println("EXCEPTION:- ", e.getMessage())
      }
    } // End Catch Block
  } //END Main()
} // END EODPosCalc Object
