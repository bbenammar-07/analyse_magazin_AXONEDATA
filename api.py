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

from fastapi import Query

@app.get("/top-spenders", response_model=List[TopSpender], summary="Obtenir les clients ayant le plus dépensé")
def get_top_spenders(limit: int = Query(10, ge=1, le=100, description="Nombre de top spenders à retourner")):
    """
    Exécute une requête SQL pour calculer le montant total dépensé par chaque client
    (en utilisant le montant total après réduction) et retourne les 'limit' premiers.
    
    Paramètres :
    - **limit** : nombre de top spenders à retourner (1-100)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = f"""
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
                LIMIT {limit};
            """
            cur.execute(query)
            results = cur.fetchall()
            
            top_spenders = [
                TopSpender(
                    user_id=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    total_spent=float(row[3])
                )
                for row in results
            ]
            
            return top_spenders

    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"Service Unavailable: {e}")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête d'analyse: {e}")
        raise HTTPException(status_code=500, detail="Error executing analysis query")
    finally:
        if conn:
            conn.close()
# Modèle Pydantic pour le produit le plus vendu
class TopProduct(BaseModel):
    product_id: int
    title: str
    total_quantity_sold: int
    total_revenue: float

@app.get("/top-products", response_model=List[TopProduct], summary="Produit le plus vendu")
def get_top_products(limit: int = Query(1, ge=1, le=20, description="Nombre de produits à retourner")):
    """
    Retourne le(s) produit(s) le plus vendu(s) avec la quantité totale et le revenu total.
    
    Paramètres :
    - **limit** : nombre de produits à retourner (1-20)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = """
                SELECT
                    product_id,
                    title,
                    SUM(quantity) AS total_quantity_sold,
                    SUM(total) AS total_revenue
                FROM cart_products
                GROUP BY product_id, title
                ORDER BY total_quantity_sold DESC
                LIMIT %s;
            """
            cur.execute(query, (limit,))
            results = cur.fetchall()
            
            return [
                TopProduct(
                    product_id=row[0],
                    title=row[1],
                    total_quantity_sold=row[2],
                    total_revenue=float(row[3])
                )
                for row in results
            ]
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des produits: {e}")
        raise HTTPException(status_code=500, detail="Error executing top products query")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)