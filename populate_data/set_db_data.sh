#!/bin/bash
docker compose down 
docker compose up -d
sleep 5
python3.10 create_schema.py
python3.10 populate_db.py
