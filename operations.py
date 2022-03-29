import time

from pymongo import MongoClient
from copy import deepcopy

port_number = int(input("Enter the port number: "))
client = MongoClient("mongodb://localhost:" + str(port_number) + "/")
db = client["291db"]


def search_titles():
    # Collection references
    collection_names = db["name_basics"]
    collection_title_b = db["title_basics"]
    collection_title_r = db["title_ratings"]
    collection_title_p = db["title_ratings"]

    collection_title_b.create_index([("startYear", 1)])
    collection_title_b.create_index([("tconst",1)])
    # Take input
    keywords = input("Enter the keywords seperated by spaces: ").strip()
    keyword_list = keywords.split()
    year = None
    match_list1 = []
    for keyword in keyword_list:
        if keyword.isdecimal():
            year = int(keyword)
        else:
            match_list1.append({
                "primaryTitle": {"$regex": keyword, "$options": "i"}
            })

    match_list2 = deepcopy(match_list1)
    if year is not None:
        match_list1.append({"startYear": year})
        match_list2.append({"primaryTitle": {"$regex": str(year), "$options": "i"}})

    query = [{
        "$match": {
            "$or": [{"$and": match_list1}, {"$and": match_list2}]
        }
    }]

    cur = collection_title_b.aggregate(query)
    docs = list(cur)
    if len(docs) != 0:
        print("-" * 243)
        print("| {:<6s} | {:<20s} | {:<9s} | {:<50s} | {:<50s} | {:<7} | {:<9s} | {:<7s} | {:<14s} | {:<40s} |"
              .format("Option", "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear",
                      "endYear"
                      , "runtimeMinutes", "genres"))
        print("-" * 243)
        for i in range(len(docs)):
            print("| {:<6d} | {:<20s} | {:<9s} | {:<50s} | {:<50s} | {:<7s} | {:<9s} | {:<7s} | {:<14s} | {:<40s} |"
                  .format(i + 1, docs[i]["tconst"], docs[i]["titleType"], docs[i]["primaryTitle"],
                          docs[i]["originalTitle"], str(docs[i]["isAdult"]), str(docs[i]["startYear"]),
                          str(docs[i]["endYear"]), str(docs[i]["runtimeMinutes"]), str(docs[i]["genres"])))
        print("-" * 243)
    else:
        print("No matches found! Try again")


def search_genres():
    # Collection references
    collection_title_b = db["title_basics"]
    collection_title_r = db["title_ratings"]

    genre = input("Enter the genre to search: ")
    min_count = int(input("Enter the minimum vote count: "))

    query = [{
        "$lookup": {
            "from": "title_ratings",
            "localField": "tconst",
            "foreignField": "tconst",
            "as": "ratings"
        }
    },
        {
            "$replaceRoot": {"newRoot": {"$mergeObjects": [{"$arrayElemAt": ["$ratings", 0]}, "$$ROOT"]}}
        },
        {
            "$project": {"ratings": 0}
        }, {
            "$unwind": "$genres"
        }, {
            "$match": {
                "numVotes": {
                    "$gte": min_count
                },
                "genres": {
                    "$regex": genre, "$options": "i"
                }
            }
        }, {
            "$sort": {"averageRating": -1}
        }]
    # collection_title_b.drop_indexes()
    # collection_title_r.drop_indexes()
    collection_title_b.create_index([("genres", 1)])
    collection_title_b.create_index([("tconst", 1)])
    collection_title_r.create_index([("tconst", 1)])

    print("Querying...")
    cur = collection_title_b.aggregate(query)
    print("Queried.")
    docs = list(cur)
    # Display the data
    if len(docs) != 0:
        print("-" * 96)
        print("| {:<66s} | {:>10s} | {:>10s} |".format("Title", "Rating", "Votes"))
        print("-" * 96)
        for doc in docs:
            print(
                "| {:<66s} | {:>10.1f} | {:>10d} |".format(doc["primaryTitle"], doc["averageRating"], doc["numVotes"]))
        print("-" * 96)
    else:
        print("No Results found")


def search_cast():
    global db
    # Get the cast member
    cast_name = input("Enter a name: ")

    # Query the collection for the details
    collection_name = db["name_basics"]
    collection_title_p = db["title_principals"]
    collection_title_b = db["title_basics"]
    query = {"primaryName": {"$regex": cast_name, "$options": "i"}}
    cur = collection_name.find(query)
    docs = list(cur)
    if len(docs) != 0:
        for doc in docs:
            print("*" * 60)
            print("Name: " + doc["primaryName"])
            print("nconst: " + doc["nconst"])
            professions = doc["primaryProfession"]
            print("Professions: ")
            if professions is not None:
                for profession in professions:
                    print(profession)
            else:
                print(professions)

            # Find the jobs and other details
            query = {"nconst": doc["nconst"]}
            job_char = collection_title_p.find(query, {"tconst": 1, "job": 1, "characters": 1, "_id": 0})
            print("-" * 60)

            for jc in job_char:
                query = {"tconst": jc["tconst"]}
                titles = collection_title_b.find(query, {"primaryTitle": 1, "_id": 0})
                printed_flag = False
                for title in titles:
                    if jc["job"] is not None or jc["characters"] is not None:
                        print("Primary Title: " + title["primaryTitle"])
                        print("Job: " + str(jc["job"]))
                        print("Characters: ")
                        if jc["characters"] is not None:
                            for character in jc["characters"]:
                                print(character)
                        else:
                            print(jc["characters"])
                        printed_flag = True

                if not printed_flag:
                    print("No Movie Appearances")

                print("-" * 60)
            print("*" * 60)
    else:
        print("No results found.")


def add_movie():
    pass


def add_cast():
    pass


def display_menu():
    print("*" * 60)
    print("* {:^56s} *".format("Operations"))
    print("*" * 60)
    print("* {:<56s} *".format("1. Search for titles"))
    print("* {:<56s} *".format("2. Search for Genres"))
    print("* {:<56s} *".format("3. Search for Cast Members"))
    print("* {:<56s} *".format("4. Add a Movie"))
    print("* {:<56s} *".format("5. Add a Cast Member"))
    print("* {:<56s} *".format("6. Exit"))
    print("*" * 60)


def main():
    # Display menu
    choice = None
    while choice != 6:
        display_menu()
        choice = int(input("Enter your choice: "))
        if choice == 1:
            print("*" * 60)
            print("*{:^58s}*".format("Search for titles"))
            print("*" * 60)
            search_titles()
        elif choice == 2:
            print("*" * 60)
            print("*{:^58s}*".format("Search for Genres"))
            print("*" * 60)
            search_genres()
        elif choice == 3:
            print("*" * 60)
            print("*{:^58s}*".format("Search for Cast Members"))
            print("*" * 60)
            search_cast()
        elif choice == 4:
            print("*" * 60)
            print("*{:^58s}*".format("Add a Movie"))
            print("*" * 60)
            add_movie()
        elif choice == 5:
            print("*" * 60)
            print("*{:^58s}*".format("Add a Cast Member"))
            print("*" * 60)
            add_cast()
        elif choice == 6:
            break
        else:
            print("Invalid Choice")


main()
# search_genres()
