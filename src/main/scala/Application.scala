

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, current_date, datediff, dense_rank, desc, hour, max, percent_rank, rank, round, row_number, to_date, to_timestamp, udf, when}

import java.util.Properties
import scala.language.postfixOps


object Application extends App {

  // val spark = SparkSession.builder
  //    .master("spark://192.168.251.107:7077")
  //    .appName("Spark Task 3.3")
  //    .config("spark.driver.host","192.168.251.106")
  //    .config("spark.driver.extraClassPath","C:/Users/nevzorov.KB/IdeaProjects/Spark_3_3/out/artifacts/Spark_3_3_jar/Spark_3_3.jar")
  //    .getOrCreate()

  val spark = SparkSession.builder().appName("Site analyze")
    .master("local[*]")
    .getOrCreate()
  //
  //
  //  println(s"Version spark is ${spark.version}")
  //
  //  val schema = new StructType()
  //    .add("id", IntegerType, true)
  //    .add("timestamp", LongType, true)
  //    .add("_type", StringType, true)
  //    .add("page_id", IntegerType, true)
  //    .add("tag", StringType, true)
  //    .add("sign", BooleanType, true)
  //
  //  val someData = Seq(
  //    Row(1, 1667738467000L, "click", 46, "красота", true),
  //    Row(1, 1662351175000L, "scroll", 46, "спорт", true),
  //    Row(1, 1668126431000L, "scroll", 46, "красота", true),
  //    Row(1, 1665491191000L, "scroll", 46, "красота", true),
  //    Row(1, 1653155893000L, "move", 1, "политика", true),
  //    Row(1, 1648189039000L, "scroll", 12, "политика", true),
  //    Row(1, 1643052936000L, "click", 46, "рецепты", true),
  //    Row(2, 1666756989000L, "click", 3, "красота", false),
  //    Row(2, 1666669665000L, "visit", 2, "политика", false),
  //    Row(2, 1659594836000L, "click", 1, "спорт", false),
  //    Row(2, 1652979733000L, "scroll", 22, "красота", false),
  //    Row(2, 1666356031000L, "click", 1, "авто", false),
  //    Row(2, 1645997728000L, "scroll", 3, "рецепты", false)
  //  )
  //
  //

  //
  //  var df = spark.createDataFrame(
  //    spark.sparkContext.parallelize(someData),
  //    StructType(schema)
  //  )
  //  df.printSchema()
  //
  //
  //  case class Action(id: Long, timestamp: Long, _type: String, page_id: Int, tag: String, sign: Boolean)
  //
  //  var actions = df.as[Action]
  //
  //  //·   Вывести топ-5 самых активных посетителей сайта
  //  actions.groupBy(col("id"))
  //    .count()
  //    .sort(desc("count"))
  //    .show(false)
  //
  //  //   Посчитать процент посетителей, у которых есть ЛК
  //
  //  val onlySignCount = actions
  //    .where(col("sign") === "true")
  //    .count()
  //  val totalActions = actions.count()
  //  println(onlySignCount / (totalActions * 0.01))
  //  //
  //  //
  //  //   Вывести топ-5 страниц сайта по показателю общего
  //  //   кол-ва кликов на данной странице
  //
  //  actions.filter(_._type == "click")
  //    .groupBy(col("page_id"))
  //    .count()
  //    .orderBy(desc("count"))
  //    .limit(5)
  //    .show()
  //
  //  // Добавьте столбец к фрейму данных со значением временного
  //
  //  df = actions.withColumn("period",
  //    when(hour(to_timestamp(col("timestamp") / 1000)) between(0, 4), "0-4")
  //      .when(hour(to_timestamp(col("timestamp") / 1000)) between(4, 8), "4-8")
  //      .when(hour(to_timestamp(col("timestamp") / 1000)) between(8, 12), "8-12")
  //      .when(hour(to_timestamp(col("timestamp") / 1000)) between(12, 16), "12-16")
  //      .when(hour(to_timestamp(col("timestamp") / 1000)) between(16, 20), "16-20")
  //      .when(hour(to_timestamp(col("timestamp") / 1000)) between(20, 24), "20-24"))
  //  df.show()
  //  //
  //  //
  //  //   Выведите временной промежуток на основе предыдущего задания,
  //  //   в течение которого было больше всего активностей на сайте.
  //
  //  df.groupBy(col("period"))
  //    .count()
  //    .orderBy(desc("count"))
  //    .limit(1)
  //    .show()
  //
  //
  //  val schemaUsers = new StructType()
  //    .add("id", IntegerType, true)
  //    .add("User_id", IntegerType, true)
  //    .add("full_name", StringType, true)
  //    .add("date_created", LongType, true)
  //
  //  val lkData = Seq(
  //    Row(100, 1, "Иванов Иван Иванович", 1266756989000L),
  //    Row(101, 2, "Петрова Лидия Михайловна", 1166756989000L))
  //
  //  var dfLk = spark.createDataFrame(
  //    spark.sparkContext.parallelize(lkData),
  //    StructType(schemaUsers)
  //  )
  //  dfLk.printSchema()
  //  dfLk.show()
  //
  //  val dfFull = df.join(dfLk, df("id") === dfLk("User_id"), "inner")
  //
  //  //·   Вывести фамилии посетителей, которые читали хотя
  //  // бы одну новость про спорт.
  //  dfFull.filter(col("tag") === "спорт")
  //    .select(col("full_name"))
  //    .distinct()
  //    .show()
  //
  //  //  Выведите 10% ЛК, у которых максимальная разница между
  //  //  датой создания ЛК и датой последнего посещения.
  //
  //  dfFull
  //    .filter(col("sign") === "true")
  //    .withColumn("diff", col("timestamp") - col("date_created"))
  //    .select(col("User_id"), col("diff"))
  //    .groupBy("User_id")
  //    .max("diff")
  //
  //    .show()
  //
  //
  //  //  Вывести топ-5 страниц, которые чаще
  //  //    всего посещают мужчины и топ-5 страниц,
  //  //  которые посещают чаще женщины.
  //
  //
  val getGender = udf((name: String) => {
    val splittedName = name.split(" ")
    if (splittedName(0).endsWith("ов")) {
      "Male"
    }
    else {
      "Female"
    }
  })

  spark.udf.register("getGender", getGender)
  //
  //  dfFull.withColumn("gender",
  //    getGender(col("full_name")))
  //    .filter(col("gender")==="Male")
  //    .groupBy("page_id")
  //    .count()
  //    .orderBy(desc("count"))
  //    .show()
  //
  //  dfFull.withColumn("gender",
  //    getGender(col("full_name")))
  //    .filter(col("gender")==="Female")
  //    .groupBy("page_id")
  //    .count()
  //    .orderBy(desc("count"))
  //    .show()


  import spark.implicits._

  val propsDb = Map("url" -> "jdbc:postgresql://localhost:5432/postgres", "user" -> "postgres", "password" -> "pass")

  var jdbcDFNewsActions = spark.read
    .format("jdbc")
    .options(propsDb)
    .option("dbtable", "news_actions")
    .load().as("event")

  jdbcDFNewsActions = jdbcDFNewsActions.withColumn("period",
    when(hour(to_timestamp(col("timestamp") / 1000)) between(0, 4), "0-4")
      .when(hour(to_timestamp(col("timestamp") / 1000)) between(4, 8), "4-8")
      .when(hour(to_timestamp(col("timestamp") / 1000)) between(8, 12), "8-12")
      .when(hour(to_timestamp(col("timestamp") / 1000)) between(12, 16), "12-16")
      .when(hour(to_timestamp(col("timestamp") / 1000)) between(16, 20), "16-20")
      .when(hour(to_timestamp(col("timestamp") / 1000)) between(20, 24), "20-24"))


  val jdbcDFPersonalArea = spark.read
    .format("jdbc")
    .options(propsDb)
    .option("dbtable", "personal_area")
    .load().as("area")


  //  Создайте витрину данных в Postgres со следующим содержанием
  //  1.       Id посетителя
  //  2.       Возраст посетителя
  //  3. Пол посетителя (постарайтесь описать логику
  //  вычисления пола в отдельной пользовательской функции)
  //  4.Любимая тематика новостей
  //  5. Любимый временной диапазон посещений
  //  6.       Id личного кабинета
  //  7. Разница в днях между созданием ЛК и
  //  датой последнего посещения. (-1 если ЛК нет)
  //  8.       Общее кол-во посещений сайта
  //  9.       Средняя длина сессии(сессией
  //  считаем временной промежуток, который
  //  охватывает последовательность событий,
  //  которые происходили подряд с разницей не
  //  более 5 минут).
  //  10.   Среднее кол-во активностей в рамках одной сессии


  val dfLeadTag = jdbcDFNewsActions
    .groupBy(col("id"), col("tag"))
    .count()
    .withColumn("row", row_number()
      .over(Window.partitionBy(col("id"))
        .orderBy(desc("count"))))
    .where($"row" === 1).as("lead")


  val dfLeadPeriod = jdbcDFNewsActions
    .groupBy(col("id"), col("period"))
    .count()
    .withColumn("row", row_number()
      .over(Window.partitionBy(col("id"))
        .orderBy(desc("count"))))
    .where($"row" === 1).as("leadPeriod")



  jdbcDFNewsActions
    .join(jdbcDFPersonalArea,
      jdbcDFNewsActions("id")
        ===
        jdbcDFPersonalArea("user_id"))
    .join(dfLeadTag, "id")
    .join(dfLeadPeriod, "id")
    .withColumn("diff", datediff(to_timestamp(col("event.timestamp")/1000), to_timestamp(col("area.date_created")/1000)))
    .select(col("event.id"),
      round(datediff(current_date(), to_date(col("area.birthday"))) / 365) as "age",
      getGender(col("area.full_name")) as "gender",
      col("lead.tag") as "tag",
      col("leadPeriod.period") as "period",
      col("area.id") as ("area_id"),
      col("diff") as "diff",
      max("diff") over(Window.partitionBy(col("area.id"))),
      count("diff") over(Window.partitionBy(col("event.id")))
    )
    //.groupBy("gender","tag", "period", "area_id")

    .show()


}

