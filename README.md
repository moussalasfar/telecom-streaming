📡 Telecom Billing Pipeline

Ce projet met en œuvre le traitement de données pour une entreprise télécom, en utilisant Apache Kafka, Apache Spark (Structured Streaming & Batch), et PostgreSQL. L’objectif est de traiter des événements télécom (CDR/EDR), de les tarifer en temps réel, puis d’agréger les données pour produire des indicateurs de facturation mensuels.

🧱 Architecture du Projet

![data (1)](https://github.com/user-attachments/assets/e79a4a1b-b6d5-403c-916e-630c4433766f)
🚀 Objectifs

Tarifer en temps réel les événements de communication (voix, données, SMS).

Agréger les usages par client et par periodes.

Générer des indicateurs financiers et opérationnels.

⚙️ Technologies

Apache Kafka (Streaming d’événements)

Apache Spark (Structured Streaming & Batch)

PostgreSQL (Stockage des résultats)

Metabase pour l'analyse

Docker (environnement reproductible)
