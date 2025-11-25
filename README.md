# 🚀 AxoneData Project

## 📖 Description
AxoneData est une application **Python** qui permet :
- d’extraire des données depuis l’API **DummyJSON**  
- de les stocker dans **PostgreSQL**  
- de fournir une **API FastAPI** pour analyser les clients ayant le plus dépensé
- de fournir une **API FastAPI** pour analyser les produit ayant le plus vendu
---

## 🌟 Fonctionnalités
- 📝 Extraction des **utilisateurs** et **paniers** depuis DummyJSON  
- 💾 Stockage des données dans **PostgreSQL** avec gestion des relations et clés étrangères  
- ⚡ API **FastAPI** pour récupérer les **Top Spenders** (clients ayant le plus dépensé)  
- 🐳 Conteneurisation via **Docker** pour PostgreSQL, pgAdmin, extraction et API  
- 📊 Gestion des erreurs et **logs** pour suivre l’exécution

---

## 🛠️ Prérequis
- 🐳 Docker et Docker Compose  
- 🐍 Python 3.11+ (si utilisation sans Docker)  
- 🔧 Git  

---

## 🚀 Installation et exécution

### Avec Docker
```bash
docker compose up -d --build


✔️ ETL Pipeline (Extraction → Load)
✔️ Base de données SQL relationnelle
✔️ Backend FastAPI monolithique
✔️ Docker Multi-Container Architecture
✔️ Pydantic pour la validation des modèles
