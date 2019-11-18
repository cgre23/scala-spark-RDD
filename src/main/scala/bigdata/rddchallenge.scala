/*      Big Data Engineering Code Challenge for Betsson Group Malta
*       by Christian Grech
*       Disclaimer: This is my first time using Scala, so even though the challenge was delivered I understand that
*                   there is room for improvement in make the code more efficient.
*       Output CSV files are in the directory output_destination.
*       FatJar dependency code is included in pom.xml, please run 'mvn install' from the uppermost folder 'test' to reproduce
* */

package bigdata

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.rdd.RDD._
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter
  import au.com.bytecode.opencsv.CSVParser
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._
  import java.sql.Timestamp
  import scala.collection.Map
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._
  import java.io.File

  object rddchallenge {
    // Merge partitions in one CSV file
    def merge(sourcePath: String, destPath: String): Unit =  {
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      FileUtil.copyMerge(hdfs, new Path(sourcePath), hdfs, new Path(destPath), false, hadoopConfig, null)
    }
    // Main function
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Big Data Challenge").setMaster("local")
      val sc = new SparkContext(conf)

      // Load transactions.csv as transactionRDD, identify header and remove it
      val transactionRDD= sc.textFile("src/main/resources/datasets/transactions.csv")
      val transactionHeader = transactionRDD.first()
      val transactionRDDWithoutHeader = transactionRDD.filter(_ != transactionHeader)
      // Load customers.csv as customersRDD, identify header and remove it
      val customersRDD = sc.textFile("src/main/resources/datasets/customers.csv")
      val customersHeader = customersRDD.first()
      val customersRDDWithoutHeader = customersRDD.filter(_ != customersHeader)
      // Load currency.csv as currencyRDD, identify header and remove it
      val currencyRDD = sc.textFile("src/main/resources/datasets/currency.csv")
      val currencyHeader = currencyRDD.first()
      val currencyRDDWithoutHeader = currencyRDD.filter(_ != currencyHeader)

      // Map Transaction RDD to another RDD having three columns (CustomerID, TransactionAmount, TransactionType)
      val transactionRDDreduced = transactionRDDWithoutHeader.map(line => {
        val colArray = line.split(",")
        (colArray(1), colArray(4).toDouble, colArray(5))
      })

      // Process the transaction amount based on the Transaction type, which will be the tag in this case
      val transactionRDDtags = transactionRDDreduced.map(item=>{
        val tag = item._3                                                   // position of tag
        val outputValue = if(tag.equals("bet") && item._2>0) item._2 * -1   // only if tag is 'bet' and the value is positive
        else if(tag.equals("withdraw") && item._2>0) item._2 * -1           // only if tag is 'withdraw' and the value is positive
        else item._2                                                        // else the transaction amount remains positive
        (item._1 ,outputValue.toDouble)
      })
      //updatedRDD.take(5).foreach(println)

      // Aggregate By Customer ID, creating (Customer ID, (Count, Total Amount)). The count is for debugging purposes
      val transactionRDDaggregate = transactionRDDtags.aggregateByKey((0,0.0d))((x,y)=>(x._1+1,x._2+y),(x,y)=>(x._1+y._1,x._2+y._2))

      // Clean the RDD by rounding money amounts and sorting RDD by Customer ID
      val transactionRDDclean = transactionRDDaggregate.map(item=>{
        val outputValue = BigDecimal(item._2._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        (item._1.toInt ,outputValue)})
      val transactionRDDsort = transactionRDDclean.sortBy(_._1, ascending = true)

      // Create timestamp
      val transactionRDDaddTime = transactionRDDsort.map { item =>
        val timestamp = Timestamp.valueOf(LocalDateTime.now)
        (item._1, item._2, timestamp)
      }
      // Output RDD to CSV file
      val file1 = "partitions/problem1partitions/"                          // Location for supported partitioned files
      val destinationFile1= "output_destination/customers_balance.csv"      // Location for single CSV file
      FileUtil.fullyDelete(new File(file1))                                 // Remove files if they are already existing
      FileUtil.fullyDelete(new File(destinationFile1))
      val header = sc.parallelize(Array("Customerid, Balance, Calculation_date"))                                             // Create header
      val customerBalance = transactionRDDaddTime.map { case (key, value, time) => Array(key, value, time).mkString(",") }    // Format data into strings
      val customerBalanceWithHeader = header.union(customerBalance)                                                           // Join header with data
      customerBalanceWithHeader.saveAsTextFile(file1)
      merge(file1, destinationFile1)                                                                                          // Merge partitioned files into one CSV file


      // Problem 2 starts here. The transaction RDD is filtered to remove withdraws and deposits as these do not contribute to net profit (my assumption)
      val  transactionRDDprofit = transactionRDDWithoutHeader.filter(line => line.contains("bet") || line.contains("win"))

      // Create RDD with Currency as Key : (Currency, (Amount, CustID, Type))
      val transactionRDDcurrency = transactionRDDprofit.map(line => {
        val colArray = line.split(",")                                  // delimiter is a comma
        (colArray(3), (colArray(4).toDouble, colArray(1), colArray(5)))
      })

      // Create Lookup table RDD for currency conversion
      val currencyRDDlookup = currencyRDDWithoutHeader.map(line=>{
        val colArray = line.split(",")
        (colArray(0), colArray(1).toDouble)
      })
      // Join the currency and transaction RDDs based on the Currency column as Key. This gives you: (Currency, (TransactionAmount, CustID, TransactionType), Currency_rate)
      val transactionRDDcurrencyJoin = transactionRDDcurrency.join(currencyRDDlookup)

      // Process the transactionRDDcurrencyJoin RDD to the format (CustID, TransactionAmount/Currency_rate, TransactionType) where the transaction amount is exchanged to EUR
      val transactionRDDexchange = transactionRDDcurrencyJoin.map(line =>{
        val cal = line._2._1._1 / line._2._2      // Exchange the transaction amount to EUR by dividing the foreign currency by the rate provided in currencyRDD
        (line._2._1._2, cal, line._2._1._3)
      })

      // Map the RDD to a (CustomerID, Profit) format so that it can be aggregated by key in the next step
      val calculateProfitRDD = transactionRDDexchange.map(item=>{
        val tag = item._3 // position of your tag
        val profitValue = if(tag.equals("bet") && item._2>0) item._2 - (item._2 * 0.01)                   // in case of a Bet, the company profits the money minus the 1 % fee
        else  item._2 * -1                                              // in case of a Win, the company loses the amount and so we add a minus sign
        (item._1 ,profitValue.toDouble)
      })

      // Aggregate by Key to get (Customer ID, Transaction Count and Total Transaction Amount). Transaction Count is included for debugging purposes
      val profitByCustomer = calculateProfitRDD.aggregateByKey((0,0.0d))((x,y)=>(x._1+1,x._2+y),(x,y)=>(x._1+y._1,x._2+y._2))

      // Clean the RDD by rounding money amounts and sorting RDD by Customer ID
      val profitRDDclean = profitByCustomer.map(item=>{
        (item._1.toInt ,item._2._2)})
      val profitRDDsorted = profitRDDclean.sortBy(_._1, ascending = true)

      // Create Customers RDD with (Customer ID, Country) by reducing the original customersRDD
      val customersRDDreduced = customersRDDWithoutHeader.map(line => {
        val colArray = line.split(",")
        (colArray(0).toInt, colArray(3))
      })
      // Join the profitRDDsorted and customerRDDreduced to get (CustomerID, (Profit, Country)) Key-Value pair and map to create a (Key, Value) pair with (Country, Profit)
      val joinedRDD = profitRDDsorted.join(customersRDDreduced).map(item=>{
        (item._2._2, item._2._1)
      })

      // Aggregate Profit by Country
      val profitRDDbyCountry = joinedRDD.aggregateByKey((0,0.0d))((x,y)=>(x._1+1,x._2+y),(x,y)=>(x._1+y._1,x._2+y._2))

      // Clean the RDD by rounding money amounts and adding timestamp
      val profitRDDround = profitRDDbyCountry.map(item=>{
        val output = BigDecimal(item._2._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val timestamp = Timestamp.valueOf(LocalDateTime.now)
        (item._1 ,output, timestamp)})

      val file = "partitions/problem2partitions/"                                               // Location for supported partitioned files
      val destinationFile= "output_destination/net_country_profit.csv"
      FileUtil.fullyDelete(new File(file))                                                      // Remove files if they are already existing
      FileUtil.fullyDelete(new File(destinationFile))
      val headerCountry = sc.parallelize(Array("Country, Net_profit_amount, Calculation_date"))
      val netCountryProfit = profitRDDround.map { case (key, value, time) => Array(key, value, time).mkString(",") }
      val netCountryProfitWithHeader = headerCountry.union(netCountryProfit)
      netCountryProfitWithHeader.saveAsTextFile(file)
      merge(file, destinationFile)    // Merge partitioned files

      /*       End of main function                                */


    }


  }