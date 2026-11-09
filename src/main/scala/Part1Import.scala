import com.mongodb.client.MongoClients
import java.io.PrintWriter
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.Date
import org.bson.Document
import scala.io.Source
import scala.jdk.CollectionConverters._

object Part1Import {

  val CSV_FILE = "hw2-data.csv"
  val JSON_OUT = "students.json"
  val MONGO_URI = "mongodb://hamza:hamza@localhost:27017/?authSource=admin"
  val DB_NAME = "university"
  val COLLECTION_NAME = "students"

  def parseLine(row: String): Document = {
    val parts = row.split(",")
    val studentId = parts(0).toInt
    val firstName = parts(1)
    val lastName = parts(2)

    val localDate = LocalDate.parse(parts(3))
    val dob = Date.from(localDate.atStartOfDay().toInstant(ZoneOffset.UTC))

    val courseIds = parts.slice(4, parts.length - 1)
      .map(_.replaceAll("[^0-9]", "").toInt)
      .toList.asJava

    val gpa = parts.last.toDouble

    new Document("_id", studentId)
      .append("firstName", firstName)
      .append("lastName", lastName)
      .append("dateOfBirth", dob)
      .append("courseIds", courseIds)
      .append("gpa", gpa)
  }

  def parseCSV(csvPath: String): List[Document] = {
    val source = Source.fromFile(csvPath, "utf-8")
    val cleanLines = source.getLines().map(_.trim).filter(_.nonEmpty).toList
    source.close()
    cleanLines.tail.map(parseLine)
  }

  def main(args: Array[String]): Unit = {
    println("_" * 60)
    println("Part 1: Import and Index Student Data")
    println("_" * 60)

    val docs = parseCSV(CSV_FILE)
    val client = MongoClients.create(MONGO_URI)

    try {
      val db = client.getDatabase(DB_NAME)
      val students = db.getCollection(COLLECTION_NAME)

      students.drop() // Clean stale data

      val res = students.insertMany(docs.asJava)
      println(s"Inserted ${res.getInsertedIds.size()} documents into $DB_NAME.$COLLECTION_NAME")

      // Create multikey index on courseIds and single-field on gpa using Document
      students.createIndex(new Document("courseIds", 1))
      students.createIndex(new Document("gpa", 1))

      println("\nCurrent Indexes on 'students':")
      students.listIndexes().forEach(idx =>
        println(s" - ${idx.getString("name")}: ${idx.get("key")}")
      )

      val writer = new PrintWriter(JSON_OUT, "utf-8")
      try {
        docs.foreach(doc => writer.println(doc.toJson()))
      } finally {
        writer.close()
      }
      println(s"Saved parsed rows to '$JSON_OUT'")
    } finally {
      client.close()
    }
  }
}