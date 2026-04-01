# Project Context: DSM Assignment 2 - Part 1 (Yelp NoSQL Database Design)

## Project Overview
This project involves designing, implementing, and querying two NoSQL database systems—MongoDB (document store) and Neo4j (graph database)—using the Yelp Open Dataset. The objective is to evaluate data modeling strategies (embedding vs. referencing in MongoDB, property graphs in Neo4j) and execute complex analytical queries.

## Environment & Hardware Constraints
* **OS:** Fedora Linux (KDE Plasma)
* **IDE:** Google Antigravity
* **Hardware Limit:** 16GB RAM (HP Victus). 
* **Crucial Rule:** The raw Yelp dataset is over 8GB in a `.tar` archive. **Do not load full JSON files into memory.** All Python preprocessing scripts must stream the `.tar` archive line-by-line (using `tarfile` and `json.loads` per line) to prevent out-of-memory errors.

## Data Strategy & Subsetting
To meet the requirement for a "non-trivial" data volume without crashing the Neo4j graph traversals, the dataset is being relationally subsetted by **Geography (State)**.
1. We are currently running an EDA script (`src/analyze_full_subset_EDA.py`) to find a state with a manageable but significant number of businesses and unique users.
2. The primary key for businesses is `business_id`; for users, it is `user_id`.
3. **Excluded Data:** The `photo.json` file and image dataset are intentionally excluded from the database design. The analytical queries do not require photo metadata, and this optimizes storage and import time.

## Current Codebase State
* `/data_samples/`: Contains 100-line samples of the core 5 JSON files (`business`, `review`, `user`, `tip`, `checkin`) used for schema planning.
* `src/generate_samples.py`: Script used to extract the 100-line samples.
* `src/analyze_full_subset_EDA.py`: Two-pass streaming script to map businesses to states, and reviews to users, to determine the optimal subsetting state.
* `requirements.txt`: Contains `pymongo` and `neo4j` drivers.

## Assignment Deliverables & Goals
The final submission requires a `.zip` containing a PDF report and two `.txt` files with query scripts.

### 1. Schema Design & Diagrams (Report)
* **MongoDB:** Must justify choices between **embedding** (e.g., check-ins inside business) and **referencing** (e.g., users to reviews). Must discuss read/write trade-offs and define at least **four specific indexes** optimized for the required queries.
* **Diagrams Required:** 1. E-R Diagram (MongoDB entities and relationships).
  2. Document Schema Diagram (Internal JSON structures, nested arrays).
  3. Neo4j Property Graph Model (Nodes: Users, Businesses, Categories. Edges: FRIENDS, REVIEWS, etc.).

### 2. MongoDB Queries (Aggregation Pipelines)
* Safest/least-safe cities and categories by star ratings (accounting for volume).
* Trend analysis of star ratings over time.
* Correlation between review volume and average star rating.
* Review behavior by category (rating distribution, length, useful votes).
* Impact of user tenure on review behavior.
* Elite vs. non-elite user comparison (mean rating, length, usefulness).
* Connection between check-in activity and star ratings.

### 3. Neo4j Cypher Queries
* Top 10 users with the highest direct friends (reporting friend count, review count, mean rating).
* Top 3 businesses by average star rating per state (>50 reviews).
* Top 10 users reviewing across the highest number of distinct cities.
* Category-specific user review behavior vs. overall mean rating.
* **Comparative Analysis:** Reproduce one MongoDB query in Cypher and compare complexity and expressiveness.

## Next Immediate Steps
1. Review the output of `analyze_full_subset_EDA.py` to select the anchor state.
2. Write a new Python script to stream the `.tar` file and extract the complete subset (Businesses in that state, Reviews of those businesses, Users who wrote those reviews) into a clean, new set of JSON files or directly into MongoDB.
3. Finalize the MongoDB schema design (embedding vs. referencing decisions) based on the structure in `/data_samples/`.