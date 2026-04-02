import os
import json
from neo4j import GraphDatabase

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "School#1607"
DATA_DIR = '/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data'
BATCH_SIZE = 5000

class YelpGraphLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_constraints(self):
        """Pre-prepare the database with the schema constraints defined in graph_database_schema.txt"""
        print("Creating constraints / indexes...")
        constraints = [
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT business_id_unique IF NOT EXISTS FOR (b:Business) REQUIRE b.business_id IS UNIQUE",
            "CREATE CONSTRAINT review_id_unique IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
            "CREATE CONSTRAINT category_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE"
        ]
        with self.driver.session() as session:
            for c in constraints:
                try:
                    session.run(c)
                except Exception as e:
                    print(f"Skipping constraint creation warning: {e}")
        print("Constraints complete.")

    def load_businesses_batch(self, batch):
        query = """
        UNWIND $batch AS data
        MERGE (b:Business {business_id: data.business_id})
        SET b.name = data.name,
            b.city = data.city,
            b.state = data.state,
            b.stars = toFloat(data.stars),
            b.review_count = toInteger(data.review_count)
            
        WITH b, data
        UNWIND data.categories AS cat
        MERGE (c:Category {name: cat})
        MERGE (b)-[:IN_CATEGORY]->(c)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch)

    def load_users_batch(self, batch):
        query = """
        UNWIND $batch AS data
        MERGE (u:User {user_id: data.user_id})
        SET u.name = data.name,
            u.review_count = toInteger(data.review_count),
            u.average_stars = toFloat(data.average_stars)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch)

    def load_reviews_batch(self, batch):
        query = """
        UNWIND $batch AS data
        MATCH (u:User {user_id: data.user_id})
        MATCH (b:Business {business_id: data.business_id})
        MERGE (r:Review {review_id: data.review_id})
        SET r.stars = toInteger(data.stars),
            r.date = data.date
        MERGE (u)-[:WROTE]->(r)
        MERGE (r)-[:REVIEWS]->(b)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch)

    def load_friendships_batch(self, batch):
        # We only match users that already exist in our subset database
        query = """
        UNWIND $batch AS data
        MATCH (u1:User {user_id: data.user1})
        MATCH (u2:User {user_id: data.user2})
        MERGE (u1)-[:FRIENDS_WITH]->(u2)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch)


def process_in_batches(loader):
    # ---------------------------------------------------------
    # 1. Load Businesses & Categories
    # ---------------------------------------------------------
    business_file = os.path.join(DATA_DIR, 'subset_business.json')
    print("\n--- Loading Businesses and Categories ---")
    batch = []
    count = 0
    with open(business_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            
            # Categories could be a strong or list
            cats = data.get('categories')
            if not cats:
                category_list = []
            elif isinstance(cats, str):
                category_list = [c.strip() for c in cats.split(',') if c.strip()]
            else:
                category_list = cats
                
            batch.append({
                "business_id": data.get('business_id'),
                "name": data.get('name'),
                "city": data.get('city'),
                "state": data.get('state'),
                "stars": data.get('stars'),
                "review_count": data.get('review_count'),
                "categories": category_list
            })
            
            if len(batch) >= BATCH_SIZE:
                loader.load_businesses_batch(batch)
                count += len(batch)
                print(f" Loaded {count:,} businesses...")
                batch = []
                
        if batch: # Catch remainder
            loader.load_businesses_batch(batch)
            count += len(batch)
            print(f" Loaded {count:,} businesses...")
            
    # ---------------------------------------------------------
    # 2. Load Users
    # ---------------------------------------------------------
    user_file = os.path.join(DATA_DIR, 'subset_user.json')
    print("\n--- Loading Users ---")
    batch = []
    count = 0
    with open(user_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            batch.append({
                "user_id": data.get('user_id'),
                "name": data.get('name'),
                "review_count": data.get('review_count'),
                "average_stars": data.get('average_stars')
            })
            if len(batch) >= BATCH_SIZE:
                loader.load_users_batch(batch)
                count += len(batch)
                print(f" Loaded {count:,} users...")
                batch = []
                
        if batch:
            loader.load_users_batch(batch)
            count += len(batch)
            print(f" Loaded {count:,} users...")
            
    # ---------------------------------------------------------
    # 3. Load Reviews & Relationship Connections
    # ---------------------------------------------------------
    review_file = os.path.join(DATA_DIR, 'subset_review.json')
    print("\n--- Loading Reviews & Edges (WROTE, REVIEWS) ---")
    batch = []
    count = 0
    with open(review_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            batch.append({
                "review_id": data.get('review_id'),
                "stars": data.get('stars'),
                "date": data.get('date'),
                "user_id": data.get('user_id'),
                "business_id": data.get('business_id')
            })
            if len(batch) >= BATCH_SIZE:
                loader.load_reviews_batch(batch)
                count += len(batch)
                print(f" Loaded {count:,} reviews...")
                batch = []
                
        if batch:
            loader.load_reviews_batch(batch)
            count += len(batch)
            print(f" Loaded {count:,} reviews...")

    # ---------------------------------------------------------
    # 4. Load Friendships
    # ---------------------------------------------------------
    print("\n--- Loading Friendships (FRIENDS_WITH edges) ---")
    batch = []
    count = 0
    with open(user_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            user_id = data.get('user_id')
            friends_data = data.get('friends')
            
            if not friends_data or friends_data == "None":
                continue
                
            if isinstance(friends_data, str):
                friends_list = [f.strip() for f in friends_data.split(',') if f.strip()]
            else:
                friends_list = friends_data
                
            for friend_id in friends_list:
                # Deduplicate dual-directional arrays by sorting
                u1, u2 = sorted([user_id, friend_id])
                batch.append({
                    "user1": u1,
                    "user2": u2
                })
                
            if len(batch) >= BATCH_SIZE:
                # Deduplicate within batch memory
                unique_batch_items = {f"{item['user1']}_{item['user2']}": item for item in batch}.values()
                loader.load_friendships_batch(list(unique_batch_items))
                count += len(unique_batch_items)
                print(f" Loaded {count:,} friendship edges...")
                batch = []
                
        if batch:
            unique_batch_items = {f"{item['user1']}_{item['user2']}": item for item in batch}.values()
            loader.load_friendships_batch(list(unique_batch_items))
            count += len(unique_batch_items)
            print(f" Loaded {count:,} friendship edges...")


if __name__ == "__main__":
    print("Initializing Neo4j Graph Import...")
    loader = YelpGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        loader.create_constraints()
        process_in_batches(loader)
        print("\n Graph Data Load Complete!")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        loader.close()