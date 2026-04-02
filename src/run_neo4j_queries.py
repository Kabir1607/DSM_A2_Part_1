import os
import json
from neo4j import GraphDatabase

# ==========================================
# CONFIGURATION
# ==========================================
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "School#1607" # Copied from your extraction script

OUTPUT_FILE = "/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/queries/node4j(cypher)_queries_and_outputs.txt"

# These are the analytical queries that return tabulated data
QUERIES = {
    "4.1": {
        "title": "Top 10 Users with Most Friends",
        "query": """
            MATCH (u:User)-[:FRIENDS_WITH]-(f:User)
            WITH u, count(f) AS friend_count
            ORDER BY friend_count DESC
            LIMIT 10
            RETURN u.name AS User, friend_count AS FriendCount, u.review_count AS TotalReviews, u.average_stars AS MeanRating
        """
    },
    "4.2": {
        "title": "Top 3 Businesses by Avg Rating Per State",
        "query": """
            MATCH (b:Business)-[:IN_CATEGORY]->(c:Category)
            WHERE b.review_count >= 50
            WITH b.state AS state, b, collect(c.name) AS categories
            ORDER BY b.stars DESC
            WITH state, collect({name: b.name, city: b.city, categories: categories, rating: b.stars})[0..3] AS top_businesses
            UNWIND top_businesses AS biz
            RETURN state AS State, biz.name AS Name, biz.city AS City, biz.categories AS Categories, biz.rating AS Rating
            ORDER BY State ASC
        """
    },
    "4.3": {
        "title": "Users Reviewing in the Most Distinct Cities",
        "query": """
            MATCH (u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)
            WITH u, b.city AS city, avg(r.stars) AS city_avg
            WITH u, count(city) AS city_count, collect({city: city, avg_rating: city_avg}) AS RatingsPerCity
            ORDER BY city_count DESC
            LIMIT 10
            RETURN u.name AS User, city_count AS DistinctCities, RatingsPerCity
        """
    },
    "4.4": {
        "title": "Category Behavior ('Mexican') vs Overall Behavior",
        "query": """
            MATCH (u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category {name: "Mexican"})
            WITH u, count(DISTINCT b) AS reviewed_biz_count, avg(r.stars) AS category_avg
            WHERE reviewed_biz_count >= 5
            RETURN u.name AS User, reviewed_biz_count AS BizCount, category_avg AS CategoryRating, u.average_stars AS OverallRating
            ORDER BY reviewed_biz_count DESC
            LIMIT 10
        """
    },
    "4.5": {
        "title": "MongoDB Comparison (Trend Analysis By Category)",
        "query": """
            MATCH (r:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category)
            WITH c.name AS Category, substring(r.date, 0, 4) AS Year, r.stars AS Stars
            WITH Category, Year, avg(Stars) AS AvgStarsYear, count(*) AS ReviewCount
            WHERE ReviewCount >= 500
            RETURN Category, Year, AvgStarsYear, ReviewCount
            ORDER BY Category ASC, Year ASC
            LIMIT 20
        """
    }
}

# These are the visual counterpart queries you can run in Neo4j Browser
VISUAL_QUERIES = {
    "4.1": """MATCH (u:User)-[r:FRIENDS_WITH]-(f:User)
WITH u, count(f) AS friend_count
ORDER BY friend_count DESC
LIMIT 3
MATCH (u)-[rel:FRIENDS_WITH]-(friends)
RETURN u, rel, friends LIMIT 100""",
    
    "4.2": """MATCH p=(b:Business)-[:IN_CATEGORY]->(c:Category)
WHERE b.review_count >= 50 AND b.stars >= 4.5
RETURN p LIMIT 50""",
    
    "4.3": """MATCH p=(u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)
WITH u, count(DISTINCT b.city) AS city_count
ORDER BY city_count DESC LIMIT 2
MATCH path=(u)-[:WROTE]->(:Review)-[:REVIEWS]->(b2:Business)
RETURN path LIMIT 50""",

    "4.4": """MATCH p=(u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category {name: "Mexican"})
WITH u, count(DISTINCT b) AS reviewed_biz_count
ORDER BY reviewed_biz_count DESC LIMIT 3
MATCH path=(u)-[:WROTE]->(:Review)-[:REVIEWS]->(:Business)-[:IN_CATEGORY]->(:Category {name: "Mexican"})
RETURN path LIMIT 100""",

    "4.5": """MATCH p=(r:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category)
WHERE substring(r.date, 0, 4) = "2018" AND b.stars >= 4.0
RETURN p LIMIT 50"""
}


def run_neo4j_queries():
    print(f" Connecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    outputs = {}
    
    with driver.session() as session:
        for q_id, q_data in QUERIES.items():
            print(f" Running Task {q_id}: {q_data['title']}...")
            try:
                result = session.run(q_data['query'])
                # Convert the Record objects into standard python dicts for JSON
                records = [dict(record) for record in result]
                outputs[q_id] = json.dumps(records, indent=2)
                print(f"    Done.")
            except Exception as e:
                outputs[q_id] = f"ERROR: {e}"
                print(f"    Error: {e}")
                
    driver.close()
    
    print("\n Generating Output Documents...")
    
    # 1. Constructing the Analytical Document
    analytical_document = f"""=============================================================================
NEO4J / CYPHER QUERIES AND OUTPUTS
DSM Assignment 2 - Task 4
=============================================================================

-----------------------------------------------------------------------------
QUERY 4.1: Top 10 Users with the Highest Friends
-----------------------------------------------------------------------------
```cypher
{QUERIES["4.1"]["query"].strip()}
```
>>> TABULAR OUTPUT:
{outputs["4.1"]}

-----------------------------------------------------------------------------
QUERY 4.2: Top 3 Businesses by Average Star Rating per State
-----------------------------------------------------------------------------
```cypher
{QUERIES["4.2"]["query"].strip()}
```
>>> TABULAR OUTPUT:
{outputs["4.2"]}

-----------------------------------------------------------------------------
QUERY 4.3: Top 10 Users Reviewing Across the Most Distinct Cities
-----------------------------------------------------------------------------
```cypher
{QUERIES["4.3"]["query"].strip()}
```
>>> TABULAR OUTPUT:
{outputs["4.3"]}

-----------------------------------------------------------------------------
QUERY 4.4: Category Mean vs Overall Mean (for 'Mexican' Category)
-----------------------------------------------------------------------------
```cypher
{QUERIES["4.4"]["query"].strip()}
```
>>> TABULAR OUTPUT:
{outputs["4.4"]}

-----------------------------------------------------------------------------
QUERY 4.5: MongoDB Comparison - Cypher Implementation of Trend Analysis (Query 3.2)
-----------------------------------------------------------------------------
```cypher
{QUERIES["4.5"]["query"].strip()}
```
>>> TABULAR OUTPUT:
{outputs["4.5"]}

[COMPLEXITY COMPARISON REPORT]
Expressiveness: The Cypher query is infinitely more elegant and concise than the MongoDB pipeline.
In MongoDB (Task 3.2), executing Trend Analysis required linking the Review to the Business, and ultimately 
iterating over the Category array elements. This required an awkward `$lookup` operation followed by two 
heavy `$unwind` stages just to manipulate the array structure, severely bloating the core analytical logic.
In contrast, Cypher's visual ASCII art syntax `MATCH (r:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category)` 
traverses the data naturally without explicitly mutating arrays. The index-free adjacency allows Neo4j to hop across these nodes instantly without computing massive Cartesian-product document joins, proving exactly why Property Graphs fundamentally dominate Many-to-Many data models over strictly-nested Document schemas.
=============================================================================
"""

    # 2. Constructing the Visual Document
    visual_document = f"""=============================================================================
NEO4J CYPHER GRAPH VISUALIZATION QUERIES
Paste these exactly into your Neo4j Browser (http://localhost:7474) to screenshot for your report.
=============================================================================

-----------------------------------------------------------------------------
VISUAL 4.1: Friendships of the Top 3 Users
-----------------------------------------------------------------------------
```cypher
{VISUAL_QUERIES["4.1"]}
```

-----------------------------------------------------------------------------
VISUAL 4.2: Top Businesses Path
-----------------------------------------------------------------------------
```cypher
{VISUAL_QUERIES["4.2"]}
```

-----------------------------------------------------------------------------
VISUAL 4.3: City Variety
-----------------------------------------------------------------------------
```cypher
{VISUAL_QUERIES["4.3"]}
```

-----------------------------------------------------------------------------
VISUAL 4.4: Category Engagement
-----------------------------------------------------------------------------
```cypher
{VISUAL_QUERIES["4.4"]}
```

-----------------------------------------------------------------------------
VISUAL 4.5: MongoDB Comparison - Review Category Trace
-----------------------------------------------------------------------------
```cypher
{VISUAL_QUERIES["4.5"]}
```
=============================================================================
"""

    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(analytical_document)
        print(f" Analytical data written successfully to {{OUTPUT_FILE}}!")
        
        GRAPH_OUTPUT_FILE = "/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/queries/graph_generation.txt"
        with open(GRAPH_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(visual_document)
        print(f" Visual Graph queries written to {{GRAPH_OUTPUT_FILE}}!\n")
    except Exception as e:
        print(f"File writing failed! {{e}}")

if __name__ == "__main__":
    run_neo4j_queries()
