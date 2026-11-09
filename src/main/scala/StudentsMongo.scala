import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Projections, Sorts}
import org.bson.Document

import java.io.PrintWriter
import java.text.SimpleDateFormat
import scala.io.{Source, StdIn}
import scala.jdk.CollectionConverters._

object StudentsMongo {

  val CSV_FILE = "hw2-data.csv"
  val JSON_OUT = "students.json"
  val MONGO_URI = "mongodb://hamza:hamza@localhost:27017/?authSource=admin"
  val DB_NAME = "university"
  val COLLECTION_NAME = "students"

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def parseLine(row:String): Document = {
    val parts = row.split(",")
    val studentId = parts(0).toInt
    val firstName = parts(1)
    val lastName = parts(2)
    val dateOfBirth = dateFormat.parse(parts(3))

    val courseIds = parts.slice(4, parts.length - 1)
      .map(_.replaceAll("[^0-9]", "").toInt)
      .toList.asJava

    val gpa = parts.last.toDouble

    new Document("_id", studentId)
      .append("firstName", firstName)
      .append("lastName", lastName)
      .append("dateOfBirth", dateOfBirth)
      .append("courseIds", courseIds)
      .append("gpa", gpa)
  }

  def parseCSV(csvPath: String): List[Document] = {
    val source = Source.fromFile(csvPath, "utf-8")
    val cleanLines = source.getLines().map(_.trim).filter(_.nonEmpty).toList
    source.close()
    cleanLines.tail.map(parseLine)
  }

  // Part 1: Import + Index Student Data
  def part1ImportAndIndex(client: MongoClient, docs: List[Document]): Unit = {
    println("_" * 60)
    println("Part 1: Import and Index Student Data")
    println("_" * 60)

    val db = client.getDatabase(DB_NAME)
    val students = db.getCollection(COLLECTION_NAME)

    students.drop() //Cleaning any old or stale data inside the students collection

    val res = students.insertMany(docs.asJava)
    println(s"Inserted ${res.getInsertedIds.size()} documents into $DB_NAME.$COLLECTION_NAME")

    students.createIndex(Indexes.ascending("courseIds"), new IndexOptions().name("idx_courseIds"))
    students.createIndex(Indexes.ascending("gpa"), new IndexOptions().name("idx_gpa"))

    println("\nCurrent Index on 'students': ")
    students.listIndexes().forEach(idx =>
      println(s"- ${idx.getString("name")}: ${idx.get("key")}")
    )

    // Saving the the list to students.json
    val writer = new PrintWriter(JSON_OUT, "utf-8")
    try {
      docs.foreach{ doc =>
        val dateOfBirth = dateFormat.format(doc.getDate("dateOfBirth"))
        val courseIdsList = doc.get("courseIds")
        val jsonLine =
          s"""{"_id": ${doc.getInteger("_id")},
             |"firstName": "${doc.getString("firstName")}",
             |"lastName": "${doc.getString("lastName")}",
             |"dateOfBirth": "$dateOfBirth",
             |"courseIds": $courseIdsList,
             |"gpa": ${doc.getDouble("gpa")}}""".stripMargin.replace("\n", " ")
        writer.println(jsonLine)
        writer.println(jsonLine)
      }
    } finally {
      writer.close()
    }
    println(s"Saved parsed rows to '$JSON_OUT'")
  }

  // Part 2: Retrieving Students by Course
  def part2QueryByCourse(client: MongoClient): Unit = {
    println("\n" + "_" * 60)
    println("Part 2: Retieving Students by Course")
    println("_" * 60)

    val collection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)

    var courseIdInput = 0
    var valid = false

    while (!valid) {
      print("Enter Course ID (Example: 102, 103): ")
      val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")

      try {
        courseIdInput = input.toInt
        valid = true
      } catch {
        case _: NumberFormatException =>
          println("\nInvalid Input! Please enter a valid integer for course ID")
      }
    }

    val results = collection
      .find(Filters.eq("courseIds", courseIdInput))
      .projection(Projections.include("_id", "firstName", "lastName", "gpa"))
      .sort(Sorts.ascending("lastName"))
      .asScala.toList

    println(s"\n___ Students registered in Course $courseIdInput (sorted by last name) ___")
    results.foreach { student =>
      val fullName = s"${student.getString("firstName")} ${student.getString("lastName")}"
      println(s"ID: ${student.getInteger("_id")} | Name: $fullName | GPA: ${student.getDouble("gpa")}")
    }
    println(s"Total Matches: ${results.size}")

    val explainData = collection.find(Filters.eq("courseIds", courseIdInput)).explain()
    val isIxscan = explainData.toJson().contains("IXSCAN")

    println("\n[Explain Plan Check]")
    if (isIxscan) {
      println(">> Index used: YES (IXSCAN stage found for 'courseIds').")
    } else {
      println(">> Index used: NO (COLLSCAN was used).")
    }
  }

  //Part 3: Retrieving Students by GPA && Combined query
  def part3QueryByGpa(client: MongoClient): Unit = {
    println("\n" + "_" * 60)
    println("Part 3: Retrieving Students by GPA")
    println("_" * 60)

    val collection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)

    var minGpa = 0.0
    var valid = false

    while(!valid) {
      print("Enter minimum GPA (Example: 3.5): ")
      val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")

      try {
        minGpa = input.toDouble
        valid = true
      }catch{
        case _: NumberFormatException =>
          println("Invalid Input! Please enter a valid decimal number for GPA")
      }
    }
    val gpaQuery = collection
      .find(Filters.gt("gpa", minGpa))
      .sort(Sorts.descending("gpa"))
      .asScala.toList

    println(s"\n___ Students with GPA > $minGpa (sorted by GPA descending) ___")
    gpaQuery.foreach { student =>
      val fullName = s"${student.getString("firstName")} ${student.getString("lastName")}"
      println(s"ID: ${student.getInteger("_id")} | Name: $fullName | GPA: ${student.getDouble("gpa")}")
    }
    println(s"Total Matches: ${gpaQuery.size}")

    println("\n___ [Optional]: Combined Query ___")
    print("Do you want to run the combined query? (y/n): ")
    val choice = Option(StdIn.readLine()).map(_.trim.toLowerCase).getOrElse("n")

    if (choice == "y" || choice == "yes") {

      // 1. keep asking for Course ID
      var cid = 0
      var validCid = false
      while (!validCid) {
        print("Enter Course ID: ")
        val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")
        try {
          cid = input.toInt
          validCid = true
        } catch {
          case _: NumberFormatException =>
            println("Invalid Course ID! Please enter a valid integer.")
        }
      }
      // 2. keep asking for Minimum GPA
      var minGpa = 0.0
      var validGpa = false
      while (!validGpa) {
        print("Enter minimum GPA: ")
        val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")
        try {
          minGpa = input.toDouble
          validGpa = true
        } catch {
          case _: NumberFormatException =>
            println("Invalid GPA! Please enter a valid decimal number.")
        }
      }
      val combResults = collection
        .find(Filters.and(Filters.eq("courseIds", cid), Filters.gt("gpa", minGpa)))
        .sort(Sorts.descending("gpa"))
        .asScala.toList

      combResults.foreach { s =>
        println(s"ID: ${s.get("_id")} | Name: ${s.get("firstName")} ${s.get("lastName")} | GPA: ${s.get("gpa")}")
      }
      println(s"\nMatches for Course <$cid> and <GPA > $minGpa>: [${combResults.size} matches]")
    } else {
      println("Skipping combined query.")
    }
  }

  def main(args: Array[String]): Unit = {
    val docs = parseCSV(CSV_FILE)
    val client = MongoClients.create(MONGO_URI)

    try{
      part1ImportAndIndex(client, docs)
      part2QueryByCourse(client)
      part3QueryByGpa(client)
    } finally {
      client.close()
    }
  }
}
