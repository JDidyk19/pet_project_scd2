# 🚀 SCD2 Data Pipeline

This project implements a **Slowly Changing Dimension Type 2 (SCD2)** pipeline to track historical changes in data. Its main purpose is to **preserve the history of records** instead of overwriting them.

---

## 📌 Features

* ✅ SCD Type 2 implementation
* ✅ Works with **PostgreSQL**
* ✅ Incremental data processing
* ✅ Handles:

  * New records
  * Updated records
  * Unchanged records
* ✅ Modular and clean pipeline structure

---

## ⚙️ Tech Stack

* Python 3.14
* PostgreSQL in docker container
* psycopg
* SQL

---

# 🧪 Setup

## Run docker container

```bash
docker compose up -d
```

---

## 🟢 Option 1 — Using uv (Recommended 🚀)

`uv` is a fast Python package manager.

```bash
pip install uv
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv run ingestion_data.py
uv run scd2_pipeline.py
```

---

## 🟣 Option 2 — Using Poetry

```bash
pip install poetry
poetry install
poetry shell
poetry run python ingestion_data.py
poetry run python scd2_pipeline.py
```

---

## 🔵 Option 3 — Using requirements.txt (Classic way)

```bash
python3 -m venv venv
source venv/bin/activate
venv\Scripts\activate
pip install -r requirements.txt
python3 ingestion_data.py
python3 scd2_pipeline.py
```
