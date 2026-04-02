from neo4j import GraphDatabase
import json

class YelpGraphLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_constraints(self):
        """Pre-prepare the database with the schema constraints we defined."""
        constraints = [
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT business_id_unique IF NOT EXISTS FOR (b:Business) REQUIRE b.business_id IS UNIQUE",
            "CREATE CONSTRAINT review_id_unique IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE"
        ]
        with self.driver.session() as session:
            for c in constraints:
                session.run(c)

    def load_users_batch(self, user_list):
        """
        Expects a list of dictionaries. 
        Uses UNWIND for high-performance batch insertion.
        """
        query = """
        UNWIND $batch AS data
        MERGE (u:User {user_id: data.user_id})
        SET u.name = data.name,
            u.review_count = toInteger(data.review_count),
            u.average_stars = toFloat(data.average_stars)
        """
        with self.driver.session() as session:
            session.run(query, batch=user_list)

    def load_reviews_and_connect(self, review_batch):
        """
        This creates the Review node AND the relationships to User and Business
        all in one optimized pass.
        """
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
            session.run(query, batch=review_batch)

# --- Example Usage ---
loader = YelpGraphLoader("bolt://localhost:7687", "neo4j", "your_password")
loader.create_constraints()

# Mock data example (you would loop through your JSON/CSV file and create these lists)
users = [
    {"user_id": "u1", "name": "Alice", "review_count": 5, "average_stars": 4.2},
    {"user_id": "u2", "name": "Bob", "review_count": 2, "average_stars": 3.5}
]

# Load in chunks of 1,000 to 10,000 for best performance
loader.load_users_batch(users)
loader.close()