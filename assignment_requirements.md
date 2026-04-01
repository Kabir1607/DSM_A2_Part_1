# Assignment 2 - Part 1: NoSQL Database Design & Analysis

## Course Context & General Rules
* [cite_start]**Course:** Data Science and Management (CS-3510), Ashoka University [cite: 1-2].
* [cite_start]**Task:** Design and implement NoSQL databases (MongoDB & Neo4j) using the Yelp Open Dataset [cite: 5-6].
* **Constraint:** It is NOT necessary to use all 7 million reviews. [cite_start]Subsetting is encouraged, but the data and task volumes must be "non-trivial" to score full points [cite: 8-10].
* [cite_start]**Deliverables:** A zip file containing a PDF Report (with 3 diagrams) and two `.txt` files containing MongoDB and Cypher scripts [cite: 12-18].

## Task 1: Data Acquisition and MongoDB Schema Design (20 Marks)
1. Load dataset into MongoDB. [cite_start]Design collection structure (does not need to perfectly mirror the 6 source files) [cite: 19-22].
2. [cite_start]Describe the purpose of each collection, fields, and linking identifiers [cite: 24-25].
3. Justify schema choices:
   * [cite_start]Explain whether you chose to **embed** or **reference** for each entity relationship [cite: 27-28].
   * [cite_start]Discuss read/write trade-offs for these decisions[cite: 31].
   * [cite_start]Identify at least **four indexes** to define on the collections, justified based on the queries in Task 3[cite: 35].

## Task 2: Diagrams (23 Marks)
1. [cite_start]**Entity-Relationship (E-R) Diagram (8 marks):** Represent MongoDB entities and relationships, distinguishing between embedded and referenced data [cite: 36-39].
2. [cite_start]**Document Schema Diagram (7 marks):** Show internal structures, nested sub-documents, arrays, and field types for MongoDB collections [cite: 41-43]. 
3. **Neo4j Property Graph Model (8 marks):** Define nodes (businesses, users, reviews, categories, etc.), relationships (with directions), and properties. [cite_start]Explain how non-graph data is handled [cite: 46-53].

## Task 3: MongoDB Querying (20 Marks)
*Requirement: Use aggregation pipelines. Include query and results in submission.* [cite: 54-55]
1. [cite_start]Identify safest and least-safe cities and business categories based on average star ratings, accounting for review volume[cite: 57].
2. [cite_start]Determine cities and categories showing strongest upward and downward trends in star rating over time[cite: 59].
3. [cite_start]Analyze correlation between review volume and average star rating[cite: 61].
4. [cite_start]Compare review behavior across categories (rating distribution, average length, useful votes ratio)[cite: 65].
5. [cite_start]Investigate impact of user tenure (time since joining) on review behavior (ratings given, usefulness)[cite: 68].
6. [cite_start]Compare elite vs. non-elite users: mean star rating given, mean review character length, mean useful votes received per review[cite: 73].
7. [cite_start]Explore connection between check-in activity and star ratings (do highly-visited businesses get higher ratings, and does this vary by category?) [cite: 75-76].

## Task 4: Neo4j / Cypher Querying (12 Marks)
*Requirement: Include query and results. [cite_start]Visualize results in a graph where applicable.* [cite: 78-79]
1. Find top 10 users with highest number of direct friends. Report friend count, total review count, and mean star rating given [cite: 80-81].
2. For each state, find top 3 businesses by average star rating (among businesses with >= 50 reviews). Report name, category, city, and average rating [cite: 82-83].
3. Find 10 users who reviewed businesses across the highest number of distinct cities. Report city count and mean star rating per city [cite: 87-88].
4. For a chosen category, find users who reviewed >= 5 businesses in that category. Compare their mean rating for that category against their overall mean rating [cite: 89-90].
5. Reproduce one query from the MongoDB section in Cypher. Compare complexity/expressiveness and discuss which model is better suited [cite: 92-93].