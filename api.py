from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration de la base de données (lecture des variables d'environnement)
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'axonedata'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', 5432)
}

app = FastAPI(
    title="Top Spenders API",
    description="API pour identifier les clients ayant le plus dépensé dans le magasin fictif."
)

# Modèle Pydantic pour la réponse
class TopSpender(BaseModel):
    user_id: int
    first_name: str
    last_name: str
    total_spent: float

def get_db_connection():
    """Établit une connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        raise ConnectionError("Database connection error")

@app.get("/top-spenders", response_model=List[TopSpender], summary="Obtenir les 10 clients ayant le plus dépensé")
def get_top_spenders():
    """
    Exécute une requête SQL pour calculer le montant total dépensé par chaque client 
    (en utilisant le montant total après réduction) et retourne les 10 premiers.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Requête SQL pour l'analyse
            query = """
                SELECT 
                    u.id AS user_id, 
                    u.firstName, 
                    u.lastName, 
                    SUM(c.discounted_total) AS total_spent 
                FROM 
                    carts c 
                JOIN 
                    users u ON c.user_id = u.id 
                GROUP BY 
                    u.id, u.firstName, u.lastName 
                ORDER BY 
                    total_spent DESC 
                LIMIT 10;
            """
            cur.execute(query)
            results = cur.fetchall()
            
            top_spenders = []
            for row in results:
                top_spenders.append(TopSpender(
                    user_id=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    # Convertir Decimal de PostgreSQL en float pour Pydantic
                    total_spent=float(row[3]) 
                ))
            
            return top_spenders

    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"Service Unavailable: {e}")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête d'analyse: {e}")
        raise HTTPException(status_code=500, detail="Error executing analysis query")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)