# 🎬 Netflix ETL Pipeline — Airflow + dbt + Snowflake + AWS

> A production-grade, end-to-end ETL pipeline built on AWS that automatically ingests raw Netflix data from S3, loads it into Snowflake, transforms it through dbt models, and delivers analytics-ready tables — with full Slack and email alerting on pipeline failures.

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Flow](#-pipeline-flow)
- [Setup Guide](#-setup-guide)
  - [Phase 1: AWS Infrastructure](#phase-1-aws-infrastructure)
  - [Phase 2: Snowflake Setup](#phase-2-snowflake-setup)
  - [Phase 3: Airflow Installation](#phase-3-airflow-installation)
  - [Phase 4: dbt Setup](#phase-4-dbt-setup)
  - [Phase 5: Alerting](#phase-5-alerting)
- [dbt Models](#-dbt-models)
- [DAG Overview](#-dag-overview)
- [Setbacks & Lessons Learned](#-setbacks--lessons-learned)
- [Screenshots](#-screenshots)
- [Author](#-author)

---

## 📖 Project Overview

This project builds a fully automated ETL pipeline that:

1. **Extracts** raw Netflix titles and credits CSV files from an AWS S3 bucket
2. **Transforms** the data through layered dbt SQL models (staging → dimension → fact)
3. **Loads** clean, analytics-ready tables into Snowflake
4. **Alerts** the team via Slack and email if anything fails

The dataset contains **6,137 Netflix titles** and **81,357 cast/crew credits**, enabling analytics like:
- Movies vs TV Shows share breakdown
- Top dominating actors by number of appearances
- Popularity scoring combining IMDB and TMDB metrics

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Cloud                            │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌─────────────────────┐  │
│  │  S3      │    │  EC2     │    │     Snowflake        │  │
│  │  Bucket  │───▶│  Airflow │───▶│  PROD Database      │  │
│  │          │    │  + dbt   │    │  ├── DBT_RAW         │  │
│  │  raw_files│   │          │    │  │   ├─ TITLES_RAW   │  │
│  │  ├credits│    └──────────┘    │  │   └─ CREDITS_RAW  │  │
│  │  └titles │         │          │  ├── Staging Models  │  │
│  └──────────┘         │          │  ├── Dimension Models│  │
│                        │          │  └── Fact Models     │  │
│  ┌──────────┐          │          └─────────────────────┘  │
│  │  SSM     │          │                                    │
│  │  Parameter│◀────────┘                                    │
│  │  Store   │    (secure credentials)                       │
│  └──────────┘                                               │
│                                                             │
│  ┌──────────┐    ┌──────────┐                              │
│  │  SNS     │    │  IAM     │                              │
│  │  Email   │    │  Role    │                              │
│  │  Alerts  │    │          │                              │
│  └──────────┘    └──────────┘                              │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
    ┌──────────┐
    │  Slack   │
    │  Alerts  │
    └──────────┘
```

---

## 🛠️ Tech Stack

| Technology | Purpose | Version |
|------------|---------|---------|
| **Apache Airflow** | Pipeline orchestration and scheduling | 2.9.3 |
| **dbt (data build tool)** | SQL transformations and data modeling | 1.11.8 |
| **Snowflake** | Cloud data warehouse | Standard Edition |
| **AWS EC2** | Server hosting Airflow and dbt | Ubuntu 24.04, c7i-flex.large |
| **AWS S3** | Raw data landing zone | - |
| **AWS SSM Parameter Store** | Secure credential storage | - |
| **AWS SNS** | Email failure notifications | - |
| **AWS IAM** | Permissions and access control | - |
| **Python** | Data loading scripts | 3.12 |
| **Slack** | Real-time pipeline alerts | Incoming Webhooks |

---

## 📁 Project Structure

```
netflix-etl-pipeline/
├── dags/
│   ├── Netflix_Data_Analytics.py      # Main Airflow DAG
│   ├── source_load/
│   │   ├── __init__.py
│   │   └── data_load.py               # S3 → Snowflake loader
│   └── alerting/
│       ├── __init__.py
│       ├── slack_alert.py             # Slack notification functions
│       └── callback_script.py        # Failure callback with SNS + Slack
│
└── netflix_project/                   # dbt project
    ├── dbt_project.yml
    ├── models/
    │   └── netflix/
    │       ├── stage/
    │       │   ├── src_netflix.yml        # Source definitions
    │       │   ├── stage_netflix.yml      # Model tests
    │       │   ├── SHOW_DETAILS_DIM.sql
    │       │   ├── SCORES_VOTES_DIM.sql
    │       │   └── CREDITS_DIM.sql
    │       ├── dimension/
    │       │   └── POPULARITY_DIM.sql
    │       └── fact/
    │           ├── ACTORS_DOMINATING_FACT.sql
    │           └── MOVIES_SERIES_SHARE.sql
    └── tests/
        └── SHOW_DETAILS_NOT_NULL.sql
```

---

## 🔄 Pipeline Flow

```
START
  │
  ▼
S3 Sensor: Wait for credits.csv
  │
  ▼
S3 Sensor: Wait for titles.csv
  │
  ▼
Load Data to Snowflake (Python)
  │  - Reads CSVs from S3 via boto3
  │  - Truncates existing raw tables
  │  - Writes 6,137 + 81,357 rows
  ▼
dbt: Run Staging Models
  │  - SHOW_DETAILS_DIM
  │  - SCORES_VOTES_DIM
  │  - CREDITS_DIM
  ▼
dbt: Run Fact & Dimension Models
  │  - POPULARITY_DIM
  │  - ACTORS_DOMINATING_FACT
  │  - MOVIES_SERIES_SHARE
  ▼
dbt: Run Data Quality Tests
  │  - not_null on CREDITS_DIM.ID
  │  - not_null on SHOW_DETAILS_DIM.ID
  │  - unique on SHOW_DETAILS_DIM.ID
  ▼
Slack: Send Success Notification
  │
  ▼
END

On any failure → Slack alert + SNS email to team
```

---

## 🚀 Setup Guide

### Prerequisites

- AWS Account with billing enabled
- Snowflake account (Standard Edition)
- Slack workspace
- GitHub account
- Windows PC with PuTTY installed

---

### Phase 1: AWS Infrastructure

#### 1.1 Launch EC2 Instance

1. Go to AWS Console → EC2 → Launch Instance
2. Select **Ubuntu Server 24.04 LTS**
3. Instance type: **c7i-flex.large** (2 vCPU, 4GB RAM)
4. Create key pair: `airflow-key.ppk` (PPK format for PuTTY)
5. Security group inbound rules:

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | SSH | My IP | Terminal access |
| 8080 | TCP | My IP | Airflow Web UI |
| 5432 | TCP | My IP | PostgreSQL |

6. Storage: **20GB**

#### 1.2 Assign Elastic IP

1. EC2 → Elastic IPs → Allocate
2. Associate with your instance
3. Your permanent IP will never change even after restarts

#### 1.3 Create S3 Bucket

```
Bucket name: netflix-data-analytics-{yourname}
Region: us-east-1
Block all public access: ON
```

Create folder `raw_files/` and upload `titles.csv` and `credits.csv`

#### 1.4 Create IAM Role

1. IAM → Roles → Create Role → EC2
2. Attach policies:
   - `AmazonS3FullAccess`
   - `AmazonSSMFullAccess`
   - `AmazonSNSFullAccess`
3. Name: `airflow-ec2-role`
4. Attach to EC2 instance: EC2 → Actions → Security → Modify IAM Role

#### 1.5 Store Credentials in SSM Parameter Store

Navigate to Systems Manager → Parameter Store → Create parameter:

| Name | Type | Value |
|------|------|-------|
| `/snowflake/username` | SecureString | `dbt_user` |
| `/snowflake/password` | SecureString | your dbt_user password |
| `/snowflake/accountname` | SecureString | your Snowflake account ID |

---

### Phase 2: Snowflake Setup

Log into Snowflake and run this SQL in a worksheet:

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORMER;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS PROD;

USE DATABASE PROD;
CREATE SCHEMA IF NOT EXISTS DBT_RAW;
CREATE SCHEMA IF NOT EXISTS DBT_DEV;
CREATE SCHEMA IF NOT EXISTS DBT_PROD;

CREATE USER IF NOT EXISTS dbt_user
  PASSWORD = 'YourStrongPassword!'
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = COMPUTE_WH;

GRANT ROLE TRANSFORMER TO USER dbt_user;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE PROD TO ROLE TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE PROD TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PROD TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN DATABASE PROD TO ROLE TRANSFORMER;
```

---

### Phase 3: Airflow Installation

Connect to your EC2 via PuTTY and run:

```bash
# Update server
sudo apt-get update && sudo apt-get upgrade -y

# Install PostgreSQL
sudo apt-get install -y postgresql postgresql-contrib

# Configure PostgreSQL
sudo -u postgres psql -c "CREATE USER airflow PASSWORD 'airflow';"
sudo -u postgres psql -c "CREATE DATABASE airflow OWNER airflow;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"

# Fix PostgreSQL 16 schema permissions
sudo -u postgres psql -c "GRANT ALL ON SCHEMA public TO airflow;"
sudo -u postgres psql -c "ALTER SCHEMA public OWNER TO airflow;"

# Create Python virtual environment
python3 -m venv ~/airflow-env
source ~/airflow-env/bin/activate

# Install Airflow with constraints
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[postgres,amazon,slack]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Configure Airflow to use PostgreSQL
# Edit ~/airflow/airflow.cfg:
# sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
# load_examples = False
# dags_folder = /home/ubuntu/airflow-code-demo/dags

# Initialize database
airflow db migrate

# Create admin user
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Start Airflow
airflow webserver &
airflow scheduler &
```

Access the UI at: `http://YOUR_ELASTIC_IP:8080`

---

### Phase 4: dbt Setup

```bash
# Create separate dbt virtual environment
python3 -m venv ~/dbt-env
source ~/dbt-env/bin/activate

# Install dbt-snowflake
pip install dbt-snowflake

# Initialize project
dbt init netflix_project
# Select snowflake, enter your account details

# Verify connection
cd ~/netflix_project
dbt debug  # Should show: All checks passed!
```

Configure `~/.dbt/profiles.yml`:

```yaml
netflix_project:
  outputs:
    dev:
      type: snowflake
      account: YOUR-ACCOUNT-IDENTIFIER
      user: dbt_user
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: TRANSFORMER
      database: PROD
      warehouse: COMPUTE_WH
      schema: DBT_RAW
      threads: 4
  target: dev
```

---

### Phase 5: Alerting

#### Slack Setup

1. Create a Slack app at https://api.slack.com/apps
2. Enable Incoming Webhooks
3. Add to a channel and copy the webhook URL
4. In Airflow UI → Admin → Connections → Add:
   - Conn ID: `Slack_Connection`
   - Type: `HTTP`
   - Host: `https://hooks.slack.com/services/`
   - Password: `your/webhook/token`

#### SNS Email Setup

1. AWS Console → SNS → Create topic (Standard)
2. Name: `airflow-pipeline-alerts`
3. Create subscription → Email → your email address
4. Confirm subscription from your inbox

---

## 📊 dbt Models

### Staging Layer

| Model | Source | Description |
|-------|--------|-------------|
| `SHOW_DETAILS_DIM` | TITLES_RAW | Title, type, release year, genres, runtime |
| `SCORES_VOTES_DIM` | TITLES_RAW | IMDB score, votes, TMDB popularity |
| `CREDITS_DIM` | CREDITS_RAW | Person ID, name, character, role |

### Dimension Layer

| Model | Description |
|-------|-------------|
| `POPULARITY_DIM` | Joins show details with scores for full popularity view |

### Fact Layer

| Model | Description |
|-------|-------------|
| `ACTORS_DOMINATING_FACT` | Top actors ranked by number of Netflix appearances |
| `MOVIES_SERIES_SHARE` | Movies vs TV Shows count and average ratings |

### Data Tests

```yaml
- not_null on SHOW_DETAILS_DIM.ID
- unique on SHOW_DETAILS_DIM.ID
- not_null on CREDITS_DIM.ID
```

---

## 🔁 DAG Overview

**DAG ID:** `Netflix_Data_Analytics`
**Schedule:** Daily
**Tasks:** 9

```
start
  └── credits_rawfile_sensor    # S3KeySensor - waits for credits.csv
        └── titles_rawfile_sensor  # S3KeySensor - waits for titles.csv
              └── Load_Data_Snowflake  # PythonOperator
                    └── run_stage_models   # BashOperator (dbt)
                          └── run_fact_dim_models  # BashOperator (dbt)
                                └── run_test_cases  # BashOperator (dbt test)
                                      └── slack_success_notification
                                            └── end
```

**Failure callback:** Any task failure triggers both Slack alert and SNS email.

---

## ⚠️ Setbacks & Lessons Learned

This project was built as a learning exercise. Here are the real errors encountered and how they were resolved:

### 1. PEP 668 — Externally Managed Python Environment
**Error:** `sudo pip install` blocked on Ubuntu 24.04
**Fix:** Always use Python virtual environments (`python3 -m venv`)
**Lesson:** Modern Ubuntu versions enforce PEP 668 which prevents system-wide pip installs

### 2. PostgreSQL 16 Schema Permissions
**Error:** `permission denied for schema public`
**Fix:**
```sql
GRANT ALL ON SCHEMA public TO airflow;
ALTER SCHEMA public OWNER TO airflow;
```
**Lesson:** PostgreSQL 15+ changed default schema ownership — the `public` schema is now owned by `pg_database_owner`, not `postgres`

### 3. Deprecated Airflow Operators
**Error:** `ImportError` on old operator paths
**Fix:** Updated imports:
```python
# Old (broken)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

# New (correct)
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
```

### 4. Snowflake Session Has No Current Database
**Error:** `Cannot perform TRUNCATE. This session does not have a current database`
**Fix:** Added explicit `USE DATABASE` and `USE SCHEMA` after connecting:
```python
cur.execute("USE DATABASE PROD")
cur.execute("USE SCHEMA DBT_RAW")
```

### 5. Snowflake Insufficient Privileges
**Error:** `SQL compilation error: Object does not exist`
**Fix:** Grant all required permissions to the TRANSFORMER role:
```sql
GRANT ALL PRIVILEGES ON DATABASE PROD TO ROLE TRANSFORMER;
GRANT ALL ON SCHEMA public TO ROLE TRANSFORMER;
ALTER SCHEMA public OWNER TO airflow;
```

### 6. dbt Column Name Case Sensitivity
**Error:** `invalid identifier 'IMDB_SCORE'` in MOVIES_SERIES_SHARE model
**Fix:** Column was in a different model — added a JOIN to `SCORES_VOTES_DIM`

### 7. Slack Webhook Operator Context Issue
**Error:** `task_success_slack_alert() missing 1 required positional argument: 'context'`
**Fix:** Changed function signature to use `**context` for Airflow 2.x compatibility:
```python
def task_success_slack_alert(**context):
```

### 8. Airflow DB Migration
**Error:** `airflow db init` deprecated
**Fix:** Use `airflow db migrate` instead (Airflow 2.7+)

---

## 📸 Screenshots

> Add your screenshots here by uploading them to the repo and referencing them below

### Airflow DAG Dashboard
![Airflow Dashboard](screenshots/airflow-dashboard.png)

### Pipeline Running Successfully
![Pipeline Run](screenshots/pipeline-run.png)

### Snowflake Data
![Snowflake Tables](screenshots/snowflake-tables.png)

### Slack Alerts
![Slack Alert](screenshots/slack-alert.png)

### SNS Email Alert
![SNS Email](screenshots/sns-email.png)

---

## 🗂️ Key Files Reference

### data_load.py
Reads CSV files from S3 using `boto3`, retrieves Snowflake credentials from AWS SSM Parameter Store, and loads data into Snowflake using `snowflake-connector-python` with `write_pandas`.

### Netflix_Data_Analytics.py
The main Airflow DAG file defining the full pipeline with S3 sensors, Python operators, Bash operators for dbt, and Slack notifications.

### slack_alert.py
Contains `task_fail_slack_alert` and `task_success_slack_alert` functions that send formatted messages to a Slack channel via Incoming Webhooks.

### callback_script.py
The DAG failure callback that triggers both Slack and SNS alerts when any task fails.

---

## 💡 Future Improvements

- Add AWS QuickSight dashboard connected to Snowflake for visual analytics
- Implement dbt snapshots to track changes in Netflix catalog over time
- Add more comprehensive data quality tests
- Set up CI/CD with GitHub Actions to auto-deploy DAG changes
- Add dbt documentation with `dbt docs generate`
- Implement data partitioning for larger datasets

---

## 👤 Author

**Ekemini Ime Otu**
- GitHub: [@EkeminiImeOtu](https://github.com/EkeminiImeOtu)


---

## 🙏 Acknowledgements

- Dataset: Netflix titles and credits dataset
- Apache Airflow documentation
- dbt documentation
- Snowflake documentation
- AWS documentation

---

*This project was built as part of a data engineering learning journey, covering the complete modern data stack from infrastructure setup to production-grade alerting.*
