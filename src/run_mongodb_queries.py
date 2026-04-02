import pymongo
import json
import os
import time

# ==========================================
# CONFIGURATION 
# ==========================================
DB_NAME = "yelp_db" 
MONGO_URI = "mongodb://localhost:27017/"

# The location of the file we generated earlier
OUTPUT_FILE = "/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/queries/mongodb_queries_and_outputs.txt"

def run_queries():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    outputs = {}
    
    print("Building/Verifying Indexes (Ensures lookups don't freeze) ...")
    db.reviews.create_index("business_id")
    db.reviews.create_index("user_id")
    db.businesses.create_index("categories")
    db.businesses.create_index("business_id") 
    db.users.create_index("user_id")
    db.businesses.create_index([("city", 1), ("review_count", -1), ("stars", -1)])
    print("Indexes ready!\n")

    # --- QUERY 3.1 ---
    print("Running Query 3.1: Safest/Least-Safe Cities AND Categories...")
    t0 = time.time()
    res_3_1_safe_city = list(db.businesses.aggregate([
        {"$match": {"review_count": {"$gte": 50}}},
        {"$group": {"_id": "$city", "avg_stars": {"$avg": "$stars"}, "total_reviews": {"$sum": "$review_count"}, "business_count": {"$sum": 1}}},
        {"$match": {"business_count": {"$gte": 10}}},
        {"$sort": {"avg_stars": -1, "total_reviews": -1}},
        {"$limit": 5}
    ]))
    res_3_1_unsafe_city = list(db.businesses.aggregate([
        {"$match": {"review_count": {"$gte": 50}}},
        {"$group": {"_id": "$city", "avg_stars": {"$avg": "$stars"}, "total_reviews": {"$sum": "$review_count"}, "business_count": {"$sum": 1}}},
        {"$match": {"business_count": {"$gte": 10}}},
        {"$sort": {"avg_stars": 1, "total_reviews": -1}},
        {"$limit": 5}
    ]))
    res_3_1_safe_cat = list(db.businesses.aggregate([
        {"$addFields": {"cat_array": {"$split": [{"$ifNull": ["$categories", ""]}, ", "]}}},
        {"$unwind": "$cat_array"},
        {"$match": {"review_count": {"$gte": 50}}},
        {"$group": {"_id": "$cat_array", "avg_stars": {"$avg": "$stars"}, "total_reviews": {"$sum": "$review_count"}, "business_count": {"$sum": 1}}},
        {"$match": {"business_count": {"$gte": 20}}},
        {"$sort": {"avg_stars": -1, "total_reviews": -1}},
        {"$limit": 5}
    ]))
    res_3_1_unsafe_cat = list(db.businesses.aggregate([
        {"$addFields": {"cat_array": {"$split": [{"$ifNull": ["$categories", ""]}, ", "]}}},
        {"$unwind": "$cat_array"},
        {"$match": {"review_count": {"$gte": 50}}},
        {"$group": {"_id": "$cat_array", "avg_stars": {"$avg": "$stars"}, "total_reviews": {"$sum": "$review_count"}, "business_count": {"$sum": 1}}},
        {"$match": {"business_count": {"$gte": 20}}},
        {"$sort": {"avg_stars": 1, "total_reviews": -1}},
        {"$limit": 5}
    ]))
    t1 = time.time()
    outputs["3.1"] = (
        f"TOP 5 SAFEST CITIES:\n{json.dumps(res_3_1_safe_city, indent=2)}\n\n"
        f"TOP 5 LEAST SAFE CITIES:\n{json.dumps(res_3_1_unsafe_city, indent=2)}\n\n"
        f"TOP 5 SAFEST CATEGORIES:\n{json.dumps(res_3_1_safe_cat, indent=2)}\n\n"
        f"TOP 5 LEAST SAFE CATEGORIES:\n{json.dumps(res_3_1_unsafe_cat, indent=2)}\n\n"
        f"--- Query executed in {t1 - t0:.2f} seconds ---"
    )

    # --- QUERY 3.2 ---
    print("\nRunning Query 3.2: Trend Analysis by City AND Category...")
    t0 = time.time()
    res_3_2 = list(db.reviews.aggregate([
        {"$lookup": {"from": "businesses", "localField": "business_id", "foreignField": "business_id", "as": "biz"}},
        {"$unwind": "$biz"},
        {"$addFields": {"cat_array": {"$split": [{"$ifNull": ["$biz.categories", ""]}, ", "]}}},
        {"$unwind": "$cat_array"},
        {"$addFields": {"year": {"$substr": ["$date", 0, 4]}}},
        {"$group": {
            "_id": {"city": "$biz.city", "category": "$cat_array", "year": "$year"},
            "avg_stars_year": {"$avg": "$stars"},
            "review_count": {"$sum": 1}
        }},
        {"$match": {"review_count": {"$gte": 50}}},
        {"$sort": {"_id.city": 1, "_id.category": 1, "_id.year": 1}},
        {"$limit": 15}
    ]))
    t1 = time.time()
    outputs["3.2"] = f"{json.dumps(res_3_2, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    # --- QUERY 3.3 ---
    print("\nRunning Query 3.3: Correlation Vol vs Stars...")
    t0 = time.time()
    res_3_3 = list(db.businesses.aggregate([
        {"$bucket": {
            "groupBy": "$review_count",
            "boundaries": [0, 10, 50, 100, 500, 1000, 5000],
            "default": "5000+",
            "output": {"avg_stars": {"$avg": "$stars"}, "business_count": {"$sum": 1}}
        }}
    ]))
    t1 = time.time()
    outputs["3.3"] = f"{json.dumps(res_3_3, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    # --- QUERY 3.4 ---
    print("\nRunning Query 3.4: Category Review Behavior...")
    t0 = time.time()
    res_3_4 = list(db.reviews.aggregate([
        {"$lookup": {"from": "businesses", "localField": "business_id", "foreignField": "business_id", "as": "biz"}},
        {"$unwind": "$biz"},
        {"$addFields": {"cat_array": {"$split": [{"$ifNull": ["$biz.categories", ""]}, ", "]}}},
        {"$unwind": "$cat_array"},
        {"$addFields": {"review_length": {"$strLenCP": {"$ifNull": ["$text", ""]}}}},
        {"$group": {
            "_id": "$cat_array",
            "avg_rating": {"$avg": "$stars"},
            "avg_length": {"$avg": "$review_length"},
            "avg_useful_votes": {"$avg": "$useful"},
            "total_reviews": {"$sum": 1}
        }},
        {"$match": {"total_reviews": {"$gte": 1000}}},
        {"$sort": {"avg_length": -1}},
        {"$limit": 10}
    ]))
    t1 = time.time()
    outputs["3.4"] = f"{json.dumps(res_3_4, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    # --- QUERY 3.5 ---
    print("\nRunning Query 3.5: User Tenure...")
    t0 = time.time()
    res_3_5 = list(db.users.aggregate([
        {"$addFields": {"join_year": {"$toInt": {"$substr": ["$yelping_since", 0, 4]}}, "dataset_end_year": 2022}},
        {"$addFields": {"tenure_years": {"$subtract": ["$dataset_end_year", "$join_year"]}}},
        {"$bucket": {
            "groupBy": "$tenure_years",
            "boundaries": [0, 2, 5, 10, 15],
            "default": "15+",
            "output": {"avg_stars_given": {"$avg": "$average_stars"}, "avg_useful_given": {"$avg": "$useful"}, "user_count": {"$sum": 1}}
        }}
    ]))
    t1 = time.time()
    outputs["3.5"] = f"{json.dumps(res_3_5, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    # --- QUERY 3.6 ---
    print("\nRunning Query 3.6: Elite vs Non-Elite...")
    t0 = time.time()
    res_3_6 = list(db.reviews.aggregate([
        {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "user_id", "as": "author"}},
        {"$unwind": "$author"},
        {"$addFields": { 
            "is_elite": {"$cond": [ {"$in": ["$author.elite", [None, "", "None", []]]}, False, True ]},
            "review_length": {"$strLenCP": {"$ifNull": ["$text", ""]}}
        }},
        {"$group": {
            "_id": "$is_elite",
            "mean_star_rating": {"$avg": "$stars"},
            "mean_review_length": {"$avg": "$review_length"},
            "mean_useful_received": {"$avg": "$useful"},
            "total_reviews": {"$sum": 1}
        }}
    ]))
    t1 = time.time()
    outputs["3.6"] = f"{json.dumps(res_3_6, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    # --- QUERY 3.7 ---
    print("\nRunning Query 3.7: Checkins vs Ratings...")
    t0 = time.time()
    res_3_7_a = list(db.businesses.aggregate([
        {"$addFields": {"checkin_count": {"$size": {"$ifNull": ["$checkins", []]}}}},
        {"$bucket": {
            "groupBy": "$checkin_count",
            "boundaries": [0, 20, 100, 500, 2000],
            "default": "2000+",
            "output": {"avg_rating": {"$avg": "$stars"}, "business_count": {"$sum": 1}}
        }}
    ]))
    res_3_7_b = list(db.businesses.aggregate([
        {"$addFields": {"checkin_count": {"$size": {"$ifNull": ["$checkins", []]}}}},
        {"$addFields": {"cat_array": {"$split": [{"$ifNull": ["$categories", ""]}, ", "]}}},
        {"$unwind": "$cat_array"},
        {"$group": {"_id": "$cat_array", "avg_checkins": {"$avg": "$checkin_count"}, "avg_rating": {"$avg": "$stars"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": 50}}},
        {"$sort": {"avg_checkins": -1}},
        {"$limit": 10}
    ]))
    t1 = time.time()
    outputs["3.7"] = f"PART A: BUCKETED BY CHECK-IN VOLUME\n{json.dumps(res_3_7_a, indent=2)}\n\nPART B: TOP 10 CATEGORIES BY CHECK-INS\n{json.dumps(res_3_7_b, indent=2)}\n\n--- Query executed in {t1 - t0:.2f} seconds ---"

    print("\nAll queries executed. Generating output file from scratch...")
    
    full_document = f"""=============================================================================
MONGODB AGGREGATION QUERIES AND OUTPUTS
DSM Assignment 2 - Task 3
=============================================================================

-----------------------------------------------------------------------------
QUERY 3.1: Safest/Least-Safe Cities and Categories (by Review Volume & Stars)
-----------------------------------------------------------------------------
```javascript
// Safest Cities
db.businesses.aggregate([
  {{ $match: {{ review_count: {{ $gte: 50 }} }} }},
  {{ $group: {{ _id: "$city", avg_stars: {{ $avg: "$stars" }}, total_reviews: {{ $sum: "$review_count" }}, business_count: {{ $sum: 1 }} }} }},
  {{ $match: {{ business_count: {{ $gte: 10 }} }} }},
  {{ $sort: {{ avg_stars: -1, total_reviews: -1 }} }},
  {{ $limit: 5 }}
])

// Safest Categories
db.businesses.aggregate([
  {{ $addFields: {{ cat_array: {{ $split: [{{ $ifNull: ["$categories", ""] }}, ", "] }} }} }},
  {{ $unwind: "$cat_array" }},
  {{ $match: {{ review_count: {{ $gte: 50 }} }} }},
  {{ $group: {{ _id: "$cat_array", avg_stars: {{ $avg: "$stars" }}, total_reviews: {{ $sum: "$review_count" }}, business_count: {{ $sum: 1 }} }} }},
  {{ $match: {{ business_count: {{ $gte: 20 }} }} }},
  {{ $sort: {{ avg_stars: -1, total_reviews: -1 }} }},
  {{ $limit: 5 }}
])
```
>>> OUTPUT 3.1:
{outputs["3.1"]}

-----------------------------------------------------------------------------
QUERY 3.2: Trend Analysis of Star Ratings Over Time (by City and Category)
-----------------------------------------------------------------------------
```javascript
db.reviews.aggregate([
  {{ $lookup: {{ from: "businesses", localField: "business_id", foreignField: "business_id", as: "biz" }} }},
  {{ $unwind: "$biz" }},
  {{ $addFields: {{ cat_array: {{ $split: [{{ $ifNull: ["$biz.categories", ""] }}, ", "] }} }} }},
  {{ $unwind: "$cat_array" }},
  {{ $addFields: {{ year: {{ $substr: ["$date", 0, 4] }} }} }},
  {{ $group: {{
      _id: {{ city: "$biz.city", category: "$cat_array", year: "$year" }},
      avg_stars_year: {{ $avg: "$stars" }},
      review_count: {{ $sum: 1 }}
  }}}},
  {{ $match: {{ review_count: {{ $gte: 50 }} }} }},
  {{ $sort: {{ "_id.city": 1, "_id.category": 1, "_id.year": 1 }} }},
  {{ $limit: 15 }}
])
```
>>> OUTPUT 3.2:
{outputs["3.2"]}

-----------------------------------------------------------------------------
QUERY 3.3: Correlation Between Review Volume and Average Star Rating
-----------------------------------------------------------------------------
```javascript
db.businesses.aggregate([
  {{ $bucket: {{
      groupBy: "$review_count",
      boundaries: [0, 10, 50, 100, 500, 1000, 5000],
      default: "5000+",
      output: {{ avg_stars: {{ $avg: "$stars" }}, business_count: {{ $sum: 1 }} }}
  }}}}
])
```
>>> OUTPUT 3.3:
{outputs["3.3"]}

-----------------------------------------------------------------------------
QUERY 3.4: Review Behavior By Category
-----------------------------------------------------------------------------
```javascript
db.reviews.aggregate([
  {{ $lookup: {{ from: "businesses", localField: "business_id", foreignField: "business_id", as: "biz" }} }},
  {{ $unwind: "$biz" }},
  {{ $addFields: {{ cat_array: {{ $split: [{{ $ifNull: ["$biz.categories", ""] }}, ", "] }} }} }},
  {{ $unwind: "$cat_array" }},
  {{ $addFields: {{ review_length: {{ $strLenCP: {{ $ifNull: ["$text", ""] }} }} }} }},
  {{ $group: {{
      _id: "$cat_array",
      avg_rating: {{ $avg: "$stars" }},
      avg_length: {{ $avg: "$review_length" }},
      avg_useful_votes: {{ $avg: "$useful" }},
      total_reviews: {{ $sum: 1 }}
  }}}},
  {{ $match: {{ total_reviews: {{ $gte: 1000 }} }} }},
  {{ $sort: {{ avg_length: -1 }} }},
  {{ $limit: 10 }}
])
```
>>> OUTPUT 3.4:
{outputs["3.4"]}

-----------------------------------------------------------------------------
QUERY 3.5: Impact of User Tenure on Review Behavior
-----------------------------------------------------------------------------
```javascript
db.users.aggregate([
  {{ $addFields: {{ join_year: {{ $toInt: {{ $substr: ["$yelping_since", 0, 4] }} }}, dataset_end_year: 2022 }} }},
  {{ $addFields: {{ tenure_years: {{ $subtract: ["$dataset_end_year", "$join_year"] }} }} }},
  {{ $bucket: {{
      groupBy: "$tenure_years",
      boundaries: [0, 2, 5, 10, 15],
      default: "15+",
      output: {{ avg_stars_given: {{ $avg: "$average_stars" }}, avg_useful_given: {{ $avg: "$useful" }}, user_count: {{ $sum: 1 }} }}
  }}}}
])
```
>>> OUTPUT 3.5:
{outputs["3.5"]}

-----------------------------------------------------------------------------
QUERY 3.6: Elite vs. Non-Elite Review Behavior
-----------------------------------------------------------------------------
```javascript
db.reviews.aggregate([
  {{ $lookup: {{ from: "users", localField: "user_id", foreignField: "user_id", as: "author" }} }},
  {{ $unwind: "$author" }},
  {{ $addFields: {{ is_elite: {{ $cond: [ {{ $in: ["$author.elite", [null, "", "None", []]] }}, false, true ] }}, review_length: {{ $strLenCP: {{ $ifNull: ["$text", ""] }} }} }} }},
  {{ $group: {{
      _id: "$is_elite",
      mean_star_rating: {{ $avg: "$stars" }},
      mean_review_length: {{ $avg: "$review_length" }},
      mean_useful_received: {{ $avg: "$useful" }},
      total_reviews: {{ $sum: 1 }}
  }}}}
])
```
>>> OUTPUT 3.6:
{outputs["3.6"]}

-----------------------------------------------------------------------------
QUERY 3.7: Connection Between Check-in Activity and Star Ratings
-----------------------------------------------------------------------------
```javascript
// Part B
db.businesses.aggregate([
  {{ $addFields: {{ checkin_count: {{ $size: {{ $ifNull: ["$checkins", []] }} }} }} }},
  {{ $addFields: {{ cat_array: {{ $split: [{{ $ifNull: ["$categories", ""] }}, ", "] }} }} }},
  {{ $unwind: "$cat_array" }},
  {{ $group: {{
      _id: "$cat_array",
      avg_checkins: {{ $avg: "$checkin_count" }},
      avg_rating: {{ $avg: "$stars" }},
      count: {{ $sum: 1 }}
  }}}},
  {{ $match: {{ count: {{ $gte: 50 }} }} }},
  {{ $sort: {{ avg_checkins: -1 }} }},
  {{ $limit: 10 }}
])
```
>>> OUTPUT 3.7:
{outputs["3.7"]}
=============================================================================
"""
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(full_document)
        print(f"Document successfully generated at {OUTPUT_FILE}!\n")
    except Exception as e:
        print(f"Failed to write to the text file! {e}")

if __name__ == "__main__":
    run_queries()
