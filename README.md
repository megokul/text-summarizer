# 📝 Text Summarizer — Google Pegasus LLM Fine-Tuning Pipeline

> **A production-grade, end-to-end abstractive text summarization system** built with FastAPI, Hugging Face Transformers, and AWS S3 — featuring a fully fine-tuned **Google Pegasus LLM**, modular pipelines, DVC data versioning, MLflow experiment tracking, and centralized logging/exception handling.

---

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?logo=fastapi)
![HuggingFace](https://img.shields.io/badge/HuggingFace-Pegasus-orange?logo=huggingface)
![MLflow](https://img.shields.io/badge/MLflow-Tracking-blue?logo=mlflow)
![DVC](https://img.shields.io/badge/DVC-Data_Versioning-purple?logo=dvc)
![AWS](https://img.shields.io/badge/AWS-S3-yellow?logo=amazonaws)

---

## 📌 Overview

The **Text Summarizer** project is a complete MLOps-ready solution for abstractive text summarization. It automates:

* **Data Ingestion** — Downloading, extracting, and organizing datasets from URLs or cloud storage.
* **Data Validation** — Schema checks, quality checks, and reproducibility safeguards.
* **Data Transformation** — Tokenization, cleaning, and preparation for model training.
* **Model Training** — Full fine-tuning of **Google Pegasus LLM** for domain-specific summarization.
* **Evaluation** — ROUGE, BLEU, and loss tracking with MLflow.
* **Deployment** — FastAPI endpoints and a Jinja2-powered web interface for predictions.

Key design principles:

* Modular, maintainable architecture.
* YAML-driven configuration for reproducibility.
* Dual local/S3 storage support.
* Centralized logging and exception handling.
* DVC for dataset versioning.
* Timestamped artifacts for idempotent runs.

---

## 🚀 Features

* **Fully fine-tuned Google Pegasus LLM** for abstractive summarization.
* Modular pipelines:

  * Data Ingestion (Local/S3, ZIP extraction, DVC sync)
  * Data Validation
  * Data Transformation
  * Model Training
  * Model Evaluation
  * Prediction
* MLflow experiment tracking and metrics logging.
* Local and AWS S3 artifact storage.
* Web UI with Jinja2 templates.
* Configurable via `config.yaml`, `params.yaml`, `schema.yaml`, `templates.yaml`.

---

## 📂 Project Structure

```text
text-summarizer/
├── app.py                   # FastAPI application entry point
├── config/                  # YAML configuration files
├── data/                    # DVC-tracked datasets
├── artifacts/               # Timestamped pipeline artifacts
├── logs/                    # Centralized logs
├── templates/               # Web UI templates
├── requirements.txt         # Python dependencies
└── src/textsummarizer/      # Core source package
    ├── components/          # Pipeline stages
    ├── config/              # Config manager
    ├── constants/           # Path constants
    ├── utils/               # Utility functions
    ├── exception/           # Custom exceptions
    ├── logging/             # Logger setup
    ├── pipelines/           # Pipeline orchestration modules
```

---

## 🔁 Pipeline Flow

```text
Raw Data → Data Ingestion → Data Validation → Data Transformation → Model Training → Model Evaluation → Deployment
```

Each stage produces structured artifacts and logs for reproducibility.

---

## ⚙️ Configuration

All settings are parameterized via YAML and `.env` files.

**YAML Configs:**

* `config.yaml` — Paths, artifact locations, storage settings.
* `params.yaml` — Training parameters, hyperparameters.
* `schema.yaml` — Dataset schema definitions.
* `templates.yaml` — Report templates.

**Environment Variables:**

```dotenv
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
S3_BUCKET_NAME=
```

---

## 🧪 Running the Project

```bash
# Start the FastAPI app
uvicorn app:app --reload
```

Access UI: [http://localhost:8000](http://localhost:8000)

---

## 📈 MLflow Tracking

* Experiment: `TextSummarizerExperiment`
* Model Registry: `PegasusTextSummarizer`
* Metrics: ROUGE, BLEU, Loss

```bash
mlflow ui
```

Visit: [http://localhost:5000](http://localhost:5000)

---

## 🌐 FastAPI Endpoints

| Method | Endpoint   | Description                       |
| ------ | ---------- | --------------------------------- |
| GET    | `/`        | Web UI home page                  |
| POST   | `/train`   | Trigger Pegasus model fine-tuning |
| POST   | `/predict` | Generate abstractive summaries    |

---

## 📝 License

This project is licensed under the **MIT License**.

---

## 👨‍💻 Author

**Gokul Krishna N V**
Machine Learning Engineer — UK 🇬🇧
[GitHub](https://github.com/megokul) • [LinkedIn](https://www.linkedin.com/in/nv-gokul-krishna)

---

## 🙌 Acknowledgements

* Google Pegasus model: [Hugging Face Transformers](https://huggingface.co/transformers/)
