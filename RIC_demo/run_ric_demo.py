import pandas as pd
from neo4j import GraphDatabase
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read Aura credentials
AURA_URI = os.getenv("NEO4J_URI")
AURA_USER = os.getenv("NEO4J_USER")
AURA_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Check if the environment variables are set
if not all([AURA_URI, AURA_USER, AURA_PASSWORD]):
    print("FATAL: Please set the NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD environment variables.")
    sys.exit(1)


def get_ric_confidence_scores(driver, last_tool_id: str) -> pd.DataFrame:
    """
    Connects to Neo4j, runs the confidence score query, and returns a DataFrame.
    """
    print(f"\n--- Querying Neo4j for confidence scores based on '{last_tool_id}' ---")
    query = """
    WITH $last_tool_id AS lastToolId
    MATCH (lastTool:Tool {id: lastToolId})
    MATCH (lastTool)<-[:EXECUTED]-(:Job)-[:IN_SESSION]->(s:Session)
    WITH COLLECT(DISTINCT s) AS sessions_with_last_tool, lastTool
    WITH size(sessions_with_last_tool) AS last_tool_session_count, sessions_with_last_tool, lastTool
    UNWIND sessions_with_last_tool AS s
    MATCH (s)<-[:IN_SESSION]-(:Job)-[:EXECUTED]->(otherTool:Tool)
    WHERE otherTool <> lastTool
    WITH last_tool_session_count, otherTool, count(DISTINCT s) AS joint_session_count
    RETURN otherTool.id AS recommendedTool,
           toFloat(joint_session_count) / last_tool_session_count AS confidence_score
    ORDER BY confidence_score DESC
    LIMIT 10;
    """
    records = []
    with driver.session() as session:
        result = session.run(query, last_tool_id=last_tool_id)
        records = [{"recommendedTool": record["recommendedTool"], "confidence_score": record["confidence_score"]} for record in result]
    print(f"--- Found {len(records)} results from the database ---")
    return pd.DataFrame(records)


class UserSessionRecommender:
    def __init__(self, driver, all_tool_ids: list, alpha: float = 0.3):
        self.driver = driver
        self.alpha = alpha
        self.session_weights = pd.DataFrame(
            {'tool': all_tool_ids, 'weight': 0.0}
        ).set_index('tool')
        print("--- New User Session Started ---")

    def update_recommendations(self, last_tool_run: str):
        """
        Updates session weights and returns top recommendations.
        """
        confidence_scores = get_ric_confidence_scores(self.driver, last_tool_run).set_index('recommendedTool')
        
        print(f"\nStep Details for '{last_tool_run}':")
        print("1. Fading old weights (multiplying by alpha={})...".format(self.alpha))
        self.session_weights['weight'] *= self.alpha
        
        print("2. Blending in new confidence scores...")
        for tool, row in confidence_scores.iterrows():
            new_score = (1 - self.alpha) * row['confidence_score']
            self.session_weights.loc[tool, 'weight'] += new_score
            
        recommendations = self.session_weights.drop(last_tool_run, errors='ignore')
        return recommendations.sort_values('weight', ascending=False).head(5)

# --- Main Simulation Logic (3 STEPS) ---
if __name__ == "__main__":
    try:
        db_driver = GraphDatabase.driver(AURA_URI, auth=(AURA_USER, AURA_PASSWORD))
        with db_driver.session() as session:
            result = session.run("MATCH (t:Tool) RETURN t.id as toolId")
            ALL_TOOLS = [record["toolId"] for record in result]

        # 1. Start a new session
        recommender = UserSessionRecommender(driver=db_driver, all_tool_ids=ALL_TOOLS, alpha=0.3)
        print(f"Initialized recommender with {len(ALL_TOOLS)} tools.")

        # =================== STEP 1 ===================
        print("\n" + "="*25 + " STEP 1 " + "="*25)
        print(">>> User runs 'FastQC'")
        recommendations_1 = recommender.update_recommendations('FastQC')
        print("\nRECOMMENDATIONS AFTER STEP 1:")
        print(recommendations_1)
        print("\nInternal Session Memory (Top 5):")
        print(recommender.session_weights.sort_values('weight', ascending=False).head())

        # =================== STEP 2 ===================
        print("\n" + "="*25 + " STEP 2 " + "="*25)
        print(">>> User now runs 'Trimmomatic'")
        recommendations_2 = recommender.update_recommendations('Trimmomatic')
        print("\nRECOMMENDATIONS AFTER STEP 2:")
        print(recommendations_2)
        print("\nInternal Session Memory (Top 5):")
        print(recommender.session_weights.sort_values('weight', ascending=False).head())

        # =================== STEP 3 ===================
        print("\n" + "="*25 + " STEP 3 " + "="*25)
        print(">>> User finally runs 'MultiQC'")
        recommendations_3 = recommender.update_recommendations('MultiQC')
        print("\nRECOMMENDATIONS AFTER STEP 3:")
        print(recommendations_3)
        print("\nInternal Session Memory (Top 5):")
        print(recommender.session_weights.sort_values('weight', ascending=False).head())

        db_driver.close()
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print("Please check your Aura credentials and that the tools ('FastQC', 'Trimmomatic', 'MultiQC') exist in your database.")