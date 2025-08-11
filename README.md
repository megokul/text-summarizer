# 📝 Text Summarizer

![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-teal)
![HuggingFace](https://img.shields.io/badge/HuggingFace-Transformers-yellow)
![License: MIT](https://img.shields.io/badge/License-MIT-green)
![DVC](https://img.shields.io/badge/DVC-3.0%2B-orange)
![AWS S3](https://img.shields.io/badge/Storage-AWS%20S3-lightblue)
![MLflow](https://img.shields.io/badge/Tracking-MLflow-lightgreen)

> **Production-grade, end-to-end abstractive text summarization pipeline**
> built with FastAPI, Hugging Face Transformers, DVC, and AWS S3 —
> complete with modular components, artifact tracking, and dual local/cloud storage.

---

## 📌 Overview

**Text Summarizer** is a fully modular and MLOps-ready project that automates the process of:

* Downloading & ingesting datasets from a configurable URL
* Extracting and preparing data for training & evaluation
* Training transformer-based sequence-to-sequence models (Hugging Face)
* Evaluating and logging model performance (MLflow-ready)
* Deploying a web interface for both training and prediction (FastAPI + Jinja2)

The design follows **god-tier, production-grade principles**:

* **Modular components** with clear boundaries
* **YAML-driven configuration**
* **Centralized logging & exception handling**
* **Local + S3 dual storage**
* **DVC dataset versioning**
* **Idempotent pipeline runs** with timestamped artifacts

---

## 🚀 Features

* **FastAPI Web Interface** for triggering training and generating summaries
* **Modular Pipelines**:

  * Data Ingestion (ZIP download, local/S3 extraction, DVC sync)
  * Data Transformation
  * Model Training
  * Model Evaluation
  * Prediction
* **Local + Cloud Storage** support (AWS S3 handler with upload/download/streaming)
* **Centralized Logging** with UTC timestamps
* **Unified Exception Handling** (`TextSummarizerError`)
* **Configuration via YAML** (`config.yaml`, `params.yaml`, `schema.yaml`, `templates.yaml`)
* **Artifact Tracking** in local artifacts/ and/or S3
* **Hugging Face Transformers** integration for Seq2Seq summarization
* **DVC** for dataset versioning and reproducibility

---

## 📂 Project Structure

```
📦 text-summarizer/
├── app.py                          # FastAPI entrypoint
├── debug.py                        # Debug helper to run training pipeline
├── project_dump.py                  # Utility to dump project code in parts
├── setup.py
├── src/
│   └── textsummarizer/
│       ├── __init__.py
│       ├── app/
│       │   └── templates/
│       │       └── index.html       # Web UI
│       ├── components/              # Modular pipeline components
│       │   ├── data_ingestion.py
│       │   ├── ...
│       ├── dbhandler/               # Database/S3 handlers
│       │   ├── base_handler.py
│       │   ├── s3_handler.py
│       ├── entity/                   # Config & artifact dataclasses
│       │   ├── config_entity.py
│       │   ├── artifact_entity.py
│       ├── exception/exception.py    # Custom exception class
│       ├── logging/                  # Logging setup
│       │   ├── __init__.py
│       │   ├── app_logger.py
│       ├── pipeline/                  # Orchestration logic
│       │   ├── training_pipeline.py
│       │   ├── prediction_pipeline.py
│       ├── utils/core.py              # Shared I/O + utility functions
│
├── config.yaml
├── params.yaml
├── schema.yaml
├── templates.yaml
```

---

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Backend:** FastAPI
* **ML Framework:** Hugging Face Transformers
* **Data Handling:** DVC, AWS S3
* **Experiment Tracking:** MLflow (optional)
* **Templating:** Jinja2
* **Logging:** Python `logging` module (UTC timestamps, file + console)
* **Config Management:** `box.ConfigBox` for dot-notation YAML access

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/megokul/text-summarizer.git
cd text-summarizer
```

### 2️⃣ Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Configure Environment Variables

Create a `.env` file:

```env
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=your-region
S3_BUCKET_NAME=your-bucket
```

### 5️⃣ Run FastAPI App

```bash
uvicorn src.textsummarizer.app.api:app --reload
```

Visit: [http://localhost:8000](http://localhost:8000)

---

## 🧩 Pipeline Stages

1. **Data Ingestion**

   * Downloads dataset ZIP (local + optional S3 streaming)
   * Extracts locally & syncs to DVC directory
   * Uploads extracted dataset to S3 if enabled
   * Returns `DataIngestionArtifact` with all local/S3 paths

2. **Data Transformation**

   * Cleans, tokenizes, and preprocesses text
   * Saves processed datasets to local/S3
   * Produces transformation artifacts

3. **Model Training**

   * Loads datasets & tokenizer from artifacts
   * Trains Seq2Seq model using Hugging Face Trainer API
   * Saves final model + tokenizer locally & optionally to S3

4. **Model Evaluation**

   * Evaluates trained model on validation set
   * Logs metrics (optionally to MLflow)
   * Produces evaluation reports

5. **Prediction**

   * Loads trained model
   * Accepts input text(s) and generates summaries
   * Returns structured output (DataFrame or dict)

---

## 📜 Configuration Files

* **`config.yaml`** — Paths, directories, S3 keys, feature flags
* **`params.yaml`** — Hyperparameters, training args, batch sizes
* **`schema.yaml`** — Dataset schema definition
* **`templates.yaml`** — Template structures for reports & outputs

---

## 🌐 API Endpoints

| Method | Endpoint   | Description                          |
| ------ | ---------- | ------------------------------------ |
| GET    | `/`        | Home page (HTML form)                |
| POST   | `/train`   | Trigger end-to-end training pipeline |
| POST   | `/predict` | Summarize given text                 |

---

## 📦 Deployment

You can deploy this project using:

* **Docker**
* **AWS EC2 / ECS**
* **Any FastAPI-compatible platform**

Example with Docker:

```bash
docker build -t text-summarizer .
docker run -p 8000:8000 text-summarizer
```

---

## 📝 License

This project is licensed under the **MIT License**.

---

## 📧 Contact

**Your Name** — [your.email@example.com](mailto:iamgokul93@example.com)
GitHub: [https://github.com/your-username](https://github.com/megokul)
