import com.mongodb.client.MongoClients
import org.bson.Document
import scala.io.StdIn
import scala.jdk.CollectionConverters._

object Part2CourseQuery {

  val MONGO_URI = "mongodb://hamza:hamza@localhost:27017/?authSource=admin"
  val DB_NAME = "university"
  val COLLECTION_NAME = "students"

  def main(args: Array[String]): Unit = {
    println("_" * 60)
    println("Part 2: Retrieving Students by Course")
    println("_" * 60)

    val client = MongoClients.create(MONGO_URI)

    try {
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
            println("Invalid Input! Please enter a valid integer for course ID.")
        }
      }

      // Native Document filter and ascending sort
      val query = new Document("courseIds", courseIdInput)
      val sort = new Document("lastName", 1)

      val queryCursor = collection.find(query).sort(sort)
      val isIxscan = queryCursor.explain().toJson().contains("IXSCAN")
      val results = queryCursor.asScala.toList

      println(s"\n___ Students registered in Course $courseIdInput (sorted by last name) ___")
      results.foreach { student =>
        val fullName = s"${student.getString("firstName")} ${student.getString("lastName")}"
        println(s"ID: ${student.getInteger("_id")} | Name: $fullName | GPA: ${student.getDouble("gpa")}")
      }
      println(s"Total Matches: ${results.size}")

      println("\n[Explain Plan Check]")
      if (isIxscan) {
        println(">> Index used: YES (IXSCAN stage found for 'courseIds').")
      } else {
        println(">> Index used: NO (COLLSCAN was used).")
      }
    } finally {
      client.close()
    }
  }
}