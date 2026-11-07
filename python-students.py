import os
import json
import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING

CSV_FILE = "hw2-data.csv"
JSON_OUT = "students.json"
MONGO_URI = "mongodb://hamza:hamza@localhost:27017/?authSource=admin"
DB_NAME = "university"
COLLECTION_NAME = "students"

def parse_line(row: str):
    parts = row.split(",")
    return {
        "_id" : int(parts[0]),
        "firstName" : parts[1],
        "lastName" : parts[2],
        "dateOfBirth" : datetime.datetime.strptime(parts[3], "%Y-%m-%d"),
        "courseIds" : list(map(int, ",".join(parts[4:-1]).strip("[] ").split(","))),
        "gpa" : float(parts[-1])
    }

def parse_csv(csv_path: str):
    with open(CSV_FILE, "r", encoding="utf-8") as file:
        clean_lines = list(filter(None, map(str.strip, file)))
        return list(map(parse_line, clean_lines[1:]))

def part1_import_and_index(client: MongoClient, docs: list):
    print("_" * 60)
    print("Part-1 : Importing and Indexing Student Data")
    print("_" * 60)

    db = client[DB_NAME]
    students = db[COLLECTION_NAME]

    students.drop()

    res = students.insert_many(docs)
    print(f"Inserted {len(res.inserted_ids)} documents into {DB_NAME}.{COLLECTION_NAME}")

    students.create_index([("courseIds", ASCENDING)], name="idx_courseIds")
    students.create_index([("gpa", ASCENDING)], name="idx_gpa")

    print("\nCurrent Indexes on 'students':")
    for idx in students.list_indexes():
        print(f" - {idx['name']}: {idx['key']}")
    # Save to students.json (one JSON document per line)
    with open(JSON_OUT, "w", encoding="utf-8") as file:
        for doc in docs:
            spark_doc = {
                "_id": doc["_id"],
                "firstName": doc["firstName"],
                "lastName": doc["lastName"],
                "dateOfBirth": doc["dateOfBirth"].strftime("%Y-%m-%d"),
                "courseIds": doc["courseIds"],
                "gpa": doc["gpa"]
            }
            file.write(json.dumps(spark_doc) + "\n")
    print(f"\nSaved parsed records to '{JSON_OUT}'.")

def part2_query_by_course(client: MongoClient):
    print("\n" + "_" * 60)
    print("Part-2 : Retrieving Students by Course")
    print("_" * 60)

    collection = client[DB_NAME][COLLECTION_NAME]

    while True:
        try:
            course_id_input = int(input("Enter Course ID (example: 103, 102): ").strip())
            break
        except ValueError:
            print("Invalid Input! Please enter a valid integer for course ID")
    query = {"courseIds" : course_id_input}
    cursor = collection.find(query, {"_id" : 1, "firstName" : 1, "lastName" : 1, "gpa" : 1}).sort("lastName", ASCENDING)
    results = list(cursor)

    print(f"\n___ Students registered in Course {course_id_input} (sorted by last name) ___")
    for student in results:
        full_name = f"{student['firstName']} {student['lastName']}"
        print(f"ID: {student['_id']} | Name: {full_name} | GPA: {student['gpa']}")
    print(f"Total Matches: {len(results)}")

    explain_data = collection.find(query).explain()
    winning_plan_str = json.dumps(explain_data.get("queryPlanner", {}).get("winningPlan", {}))
    is_ixscan = "IXSCAN" in winning_plan_str
    print("\n[Explain Plan Check]")
    if is_ixscan:
        print(">> Index used: YES (IXSCAN stage found for 'courseIds').")
    else:
        print(">> Index used: NO (COLLSCAN was used).")

def part3_query_by_gpa(client: MongoClient):
    print("\n" + "_" * 60)
    print("PART 3: Retrieving Students by GPA")
    print("_" * 60)

    collection = client[DB_NAME][COLLECTION_NAME]

    while True:
        try:
            gpa_input = float(input("Enter  minimum GPA (0.0 - 4.0): "))
            if 0.0 <= gpa_input <= 3.9:
                break
            print("GPA must be between 0.0 and less than 4.0")
        except ValueError:
            print("Invalid input! Please enter a valid numeric GPA.")
    query = {"gpa": {"$gt": gpa_input}}
    results = list(collection.find(query).sort("gpa", DESCENDING))

    print(f"\n___ Students with GPA > {gpa_input} (sorted by GPA descending) ___")
    for student in results:
        full_name = f"{student['firstName']} {student['lastName']}"
        print(f"ID: {student['_id']} | Name: {full_name} | GPA: {student['gpa']}")
    print(f"Total Matches: {len(results)}")

    print("\n___ [Optional]: Combined Query - (Course ID + Min GPA) ___")
    course_id_input = input("Enter Course ID (or press Enter to skip): ").strip()
    gpa_input = input("Enter minimum GPA (or press Enter to skip): ").strip()

    combined_filter = {}
    if course_id_input:
        try:
            combined_filter["courseIds"] = int(course_id_input)
        except ValueError:
            print("Invalid Course ID; skipping course filter.")
    if gpa_input:
        try:
            combined_filter["gpa"] = {"$gt": float(gpa_input)}
        except ValueError:
            print("Invalid GPA; skipping GPA filter.")

    if combined_filter:
        comb_results = list(collection.find(combined_filter).sort("gpa", DESCENDING))
        print(f"\nMatches for filter {combined_filter}: {len(comb_results)}")
        for student in comb_results:
            print(f"ID: {student['_id']} | Name: {student['firstName']} {student['lastName']} | Courses: {student['courseIds']} | GPA: {student['gpa']}")
    else:
        print("No combined filters applied.")

docs = parse_csv(CSV_FILE)
client = MongoClient(MONGO_URI)
part1_import_and_index(client, docs)
part2_query_by_course(client)
part3_query_by_gpa(client)