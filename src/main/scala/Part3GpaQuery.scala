import com.mongodb.client.MongoClients
import org.bson.Document
import scala.io.StdIn
import scala.jdk.CollectionConverters._

object Part3GpaQuery {

  val MONGO_URI = "mongodb://hamza:hamza@localhost:27017/?authSource=admin"
  val DB_NAME = "university"
  val COLLECTION_NAME = "students"

  def main(args: Array[String]): Unit = {
    println("_" * 60)
    println("Part 3: Retrieving Students by GPA & Combined Query")
    println("_" * 60)

    val client = MongoClients.create(MONGO_URI)

    try {
      val collection = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)

      var minGpa = 0.0
      var valid = false

      // GPA strictly between 0.0 and 4.0 exclusive
      while (!valid) {
        print("Enter minimum GPA (between 0.0 and 4.0 exclusive): ")
        val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")
        try {
          val parsedGpa = input.toDouble
          if (parsedGpa > 0.0 && parsedGpa < 4.0) {
            minGpa = parsedGpa
            valid = true
          } else {
            println("Invalid GPA! GPA must be strictly between 0.0 and 4.0 (exclusive).")
          }
        } catch {
          case _: NumberFormatException =>
            println("Invalid Input! Please enter a valid decimal number for GPA.")
        }
      }

      // filter: { "gpa": { "$gt": minGpa } } and descending sort
      val query = new Document("gpa", new Document("$gt", minGpa))
      val sort = new Document("gpa", -1)

      val queryCursor = collection.find(query).sort(sort)
      val isIxscan = queryCursor.explain().toJson().contains("IXSCAN")
      val results = queryCursor.asScala.toList

      println(s"\n___ Students with GPA > $minGpa (sorted by GPA descending) ___")
      results.foreach { student =>
        val fullName = s"${student.getString("firstName")} ${student.getString("lastName")}"
        println(s"ID: ${student.getInteger("_id")} | Name: $fullName | GPA: ${student.getDouble("gpa")}")
      }
      println(s"Total Matches: ${results.size}")

      println("\n[Explain Plan Check]")
      if (isIxscan) {
        println(">> Index used: YES (IXSCAN stage found for 'gpa').")
      } else {
        println(">> Index used: NO (COLLSCAN was used).")
      }

      // Optional Combined Query
      println("\n___ [Optional]: Combined Query ___")
      print("Do you want to run the combined query? (y/n): ")
      val choice = Option(StdIn.readLine()).map(_.trim.toLowerCase).getOrElse("n")

      if (choice == "y" || choice == "yes") {
        var cid = 0
        var validCid = false
        while (!validCid) {
          print("Enter Course ID (Example: 101, 102): ")
          val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")
          try {
            cid = input.toInt
            validCid = true
          } catch {
            case _: NumberFormatException =>
              println("Invalid Course ID! Please enter a valid integer.")
          }
        }

        var combGpa = 0.0
        var validGpa = false
        while (!validGpa) {
          print("Enter minimum GPA (between 0.0 and 4.0 exclusive): ")
          val input = Option(StdIn.readLine()).map(_.trim).getOrElse("")
          try {
            val parsedGpa = input.toDouble
            if (parsedGpa > 0.0 && parsedGpa < 4.0) {
              combGpa = parsedGpa
              validGpa = true
            } else {
              println("Invalid GPA! GPA must be strictly between 0.0 and 4.0 (exclusive).")
            }
          } catch {
            case _: NumberFormatException =>
              println("Invalid GPA! Please enter a valid decimal number.")
          }
        }

        val combinedFilter = new Document("courseIds", cid)
          .append("gpa", new Document("$gt", combGpa))

        val combResults = collection
          .find(combinedFilter)
          .sort(new Document("gpa", -1))
          .asScala.toList

        println(s"\nMatches for Course $cid and GPA > $combGpa: ${combResults.size}")
        combResults.foreach { s =>
          println(s"ID: ${s.get("_id")} | Name: ${s.get("firstName")} ${s.get("lastName")} | Courses: ${s.get("courseIds")} | GPA: ${s.get("gpa")}")
        }
      } else {
        println("Skipping combined query.")
      }

    } finally {
      client.close()
    }
  }
}