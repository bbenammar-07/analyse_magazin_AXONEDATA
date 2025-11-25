import requests
import psycopg2
from psycopg2.extras import execute_batch
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DummyJSONExtractor:
    """
    Classe pour extraire les donnees de l'API DummyJSON et les sauvegarder dans PostgreSQL
    """
    
    def __init__(self, db_config):
        """
        Initialisation avec la configuration de la base de donnees
        """
        self.db_config = db_config
        self.base_url = "https://dummyjson.com"
        self.conn = None
        
    def connect_db(self):
        """
        Etablir la connexion a la base de donnees PostgreSQL
        """
        try:
            # Attendre que la base de données soit prête (déjà géré par depends_on: service_healthy dans docker-compose)
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connexion a la base de donnees etablie")
        except Exception as e:
            logger.error(f"Erreur de connexion a la base de donnees: {e}")
            raise
    
    def create_tables(self):
        """
        Creation des tables necessaires pour stocker les donnees
        """
        cursor = self.conn.cursor()
        
        create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            firstName VARCHAR(100),
            lastName VARCHAR(100),
            email VARCHAR(150),
            phone VARCHAR(50),
            age INTEGER
        );
        """
        
        create_carts_table = """
        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            total DECIMAL(10, 2),
            discounted_total DECIMAL(10, 2),
            total_products INTEGER,
            total_quantity INTEGER
        );
        """
        
        create_cart_products_table = """
        CREATE TABLE IF NOT EXISTS cart_products (
            id SERIAL PRIMARY KEY,
            cart_id INTEGER REFERENCES carts(id),
            product_id INTEGER,
            title VARCHAR(255),
            price DECIMAL(10, 2),
            quantity INTEGER,
            total DECIMAL(10, 2),
            discount_percentage DECIMAL(5, 2)
        );
        """
        
        try:
            cursor.execute(create_users_table)
            cursor.execute(create_carts_table)
            cursor.execute(create_cart_products_table)
            self.conn.commit()
            logger.info("Tables creees avec succes")
        except Exception as e:
            logger.error(f"Erreur lors de la creation des tables: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def extract_users(self):
        """
        Extraction de tous les utilisateurs depuis l'API DummyJSON avec pagination
        """
        try:
            all_users = []
            limit = 100 # Augmenter la limite pour réduire les requêtes
            skip = 0
            
            while True:
                # Utiliser limit=100 et skip pour s'assurer de récupérer tous les utilisateurs
                url = f"{self.base_url}/users?limit={limit}&skip={skip}"
                response = requests.get(url)
                response.raise_for_status()
                users_data = response.json()
                
                current_users = users_data.get('users', [])
                
                if not current_users:
                    break
                
                all_users.extend(current_users)
                logger.info(f"Extraction de {len(current_users)} utilisateurs (skip={skip})")
                
                # Vérifier si on a atteint la fin (si le nombre d'éléments retournés est inférieur à la limite)
                if len(current_users) < limit:
                    break
                
                skip += limit
                
            logger.info(f"Total: {len(all_users)} utilisateurs extraits")
            return all_users
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des utilisateurs: {e}")
            raise
    
    def extract_carts(self):
        """
        Extraction de tous les paniers depuis l'API DummyJSON avec pagination
        """
        try:
            all_carts = []
            limit = 100 # Augmenter la limite pour réduire les requêtes
            skip = 0
            
            while True:
                # Utiliser limit=100 et skip pour s'assurer de récupérer tous les paniers
                url = f"{self.base_url}/carts?limit={limit}&skip={skip}"
                response = requests.get(url)
                response.raise_for_status()
                carts_data = response.json()
                
                current_carts = carts_data.get('carts', [])
                
                if not current_carts:
                    break
                
                all_carts.extend(current_carts)
                logger.info(f"Extraction de {len(current_carts)} paniers (skip={skip})")
                
                # Vérifier si on a atteint la fin (si le nombre d'éléments retournés est inférieur à la limite)
                if len(current_carts) < limit:
                    break
                
                skip += limit
            
            logger.info(f"Total: {len(all_carts)} paniers extraits")
            return all_carts
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des paniers: {e}")
            raise
    
    def save_users(self, users):
        """
        Sauvegarde des utilisateurs dans la base de donnees
        """
        cursor = self.conn.cursor()
        
        insert_query = """
        INSERT INTO users (id, firstName, lastName, email, phone, age)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            firstName = EXCLUDED.firstName,
            lastName = EXCLUDED.lastName,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            age = EXCLUDED.age;
        """
        
        users_data = [
            (user['id'], user['firstName'], user['lastName'], 
             user['email'], user['phone'], user['age'])
            for user in users
        ]
        
        try:
            execute_batch(cursor, insert_query, users_data)
            self.conn.commit()
            logger.info(f"{len(users_data)} utilisateurs sauvegardes")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des utilisateurs: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def save_carts(self, carts, existing_user_ids):
        """
        Sauvegarde des paniers et produits dans la base de donnees
        Filtre les paniers dont le user_id n'existe pas dans la table users
        """
        cursor = self.conn.cursor()
        
        insert_cart_query = """
        INSERT INTO carts (id, user_id, total, discounted_total, total_products, total_quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            total = EXCLUDED.total,
            discounted_total = EXCLUDED.discounted_total,
            total_products = EXCLUDED.total_products,
            total_quantity = EXCLUDED.total_quantity;
        """
        
        insert_product_query = """
        INSERT INTO cart_products (cart_id, product_id, title, price, quantity, total, discount_percentage)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        carts_saved = 0
        carts_skipped = 0
        
        try:
            for cart in carts:
                # CORRECTION: La logique de filtrage est correcte pour éviter la violation de clé étrangère
                if cart['userId'] not in existing_user_ids:
                    logger.warning(f"Panier {cart['id']} ignore: user_id {cart['userId']} n'existe pas")
                    carts_skipped += 1
                    continue
                
                cursor.execute(insert_cart_query, (
                    cart['id'], cart['userId'], cart['total'], 
                    cart['discountedTotal'], cart['totalProducts'], 
                    cart['totalQuantity']
                ))
                
                # Sauvegarde des produits du panier
                product_data = [
                    (
                        cart['id'], 
                        product['id'], 
                        product['title'],
                        product['price'], 
                        product['quantity'], 
                        product['total'], 
                        product['discountPercentage']
                    )
                    for product in cart['products']
                ]
                
                execute_batch(cursor, insert_product_query, product_data)
                
                carts_saved += 1
            
            self.conn.commit()
            logger.info(f"{carts_saved} paniers sauvegardes, {carts_skipped} paniers ignores")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des paniers: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def run_extraction(self):
        """
        Execution complete du processus d'extraction et de sauvegarde
        """
        try:
            self.connect_db()
            self.create_tables()
            
            users = self.extract_users()
            self.save_users(users)
            
            # Récupérer les IDs des utilisateurs existants après la sauvegarde
            existing_user_ids = set(user['id'] for user in users)
            logger.info(f"IDs utilisateurs disponibles: {len(existing_user_ids)} IDs")
            
            carts = self.extract_carts()
            self.save_carts(carts, existing_user_ids)
            
            logger.info("Extraction et sauvegarde terminees avec succes")
        except Exception as e:
            logger.error(f"Erreur lors de l'execution: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Connexion fermee")

if __name__ == "__main__":
    # Utiliser les variables d'environnement définies dans docker-compose
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'database': os.getenv('POSTGRES_DB', 'axonedata'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', 5432)
    }
    
    extractor = DummyJSONExtractor(db_config)
    extractor.run_extraction()