# Rearc Quest – Complete Project Documentation

This repository contains the full solution for the Rearc Quest assignment. The project is divided into three parts, each handling a specific part of the pipeline. Together they download public datasets, keep them synchronized in an S3 bucket, ingest population data from an external API, and run analysis combining both datasets.
All configuration values are stored in a single `.env` file at the project root. Every script automatically loads this file, so configuration always stays in one place.

---

## Project Structure
```
rearc_quest/
│
├── .env
├── requirements.txt
│
├── part1/
│   ├── bls_sync.py
│   ├── check_s3_structure.py
│   └── requirements.txt
│
├── part2/
│   ├── population_ingest.py
│   └── requirements.txt
│
├── part3/
│   ├── part3_final_cleaned.ipynb
│   ├── transform_phase1_3.py
│   └── requirements.txt
│
└── part4/
    ├── app.py
    ├── cdk_stack.py
    ├── cdk.json
    ├── requirements.txt
    │
    ├── lambda/
    │   ├── ingestion_handler.py
    │   ├── analytics_handler.py
    │   └── requirements.txt
    │
    ├── Dockerfile
    ├── docker-compose.yml
    ├── docker-cdk.ps1

```

Each part is isolated and contains only the dependencies it requires. The root-level `requirements.txt` file includes all dependencies for users who prefer installing everything at once.

---

## Environment Configuration
All configuration lives in a single `.env` file at the project root. Each script loads it using the same logic to ensure consistent configuration regardless of where the script is executed.
Example `.env` file:
AWS_BUCKET_NAME=rearc-bls
AWS_BUCKET_PREFIX=bls-data/
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
BLS_BASE_URL=https://download.bls.gov/pub/time.series/pr/
POPULATION_API_URL=https://datausa.io/api/data?drilldowns=Nation&measures=Population
CONTACT_EMAIL=youremail@address.com

The `.env` file should never be committed to version control.

---

## Installing Dependencies

To install all dependencies at once:
pip install -r requirements.txt

**Note:** But its recommended to install all the dependencies for individual part while running their code, which will look like:
```bash
cd part1 && pip install -r requirements.txt
cd part2 && pip install -r requirements.txt
cd part3 && pip install -r requirements.txt
```
---

# Part 1: BLS Dataset Synchronization

Location: `part1/bls_sync.py`

This script synchronizes the BLS PRS time-series dataset into an S3 bucket.  
It not only downloads the files but also keeps the S3 folder in sync with the source, including additions, deletions, and updates.

### What the Script Does

- Scrapes the BLS directory and discovers all files dynamically.
- Handles potential 403 errors by setting required request headers.
- Downloads each file and calculates its MD5 hash.
- Compares the MD5 hash with the S3 object's ETag to detect changes.
- Uploads a file only when it is new or has changed.
- Keeps the S3 bucket clean by deleting files that were removed from the source.
- Does not rely on hardcoded filenames; it adapts to new or removed files automatically.

### Running the Script
```bash
cd part1
python bls_sync.py
```
After running, your S3 bucket contains a complete and up-to-date mirror of all BLS `.txt` files in a folder called bls-data in your S3 bucket.


---

# Part 2: Population API Ingest

Location: `part2/population_ingest.py`

This script fetches United States population data from the DataUSA API and stores the output as a JSON file in the same S3 bucket used in Part 1. The script is designed so that it only uploads data when something has actually changed.

### What the Script Does

- Sends a request to the DataUSA population API.
- Parses and serializes the JSON in a deterministic format using sorted keys.
- Computes an MD5 hash of the JSON content.
- Compares this hash with the ETag of the existing object in S3.
- Skips the upload if the content is identical to what is currently stored.
- Uploads the file only when the API data has changed.
- Uses the same root-level `.env` configuration for AWS credentials, region, bucket, and API URL.

The behaviors above ensure that:
- No redundant uploads occur.
- The JSON stored in S3 is always the freshest available.
- The data is consistent and predictable for downstream processing.

### Running the Script

To run the ingestion workflow:
```bash
cd part2
python population_ingest.py
```

The output will be stored in the S3 bucket as: population_data.json

---

# Part 3: Data Analysis and Reporting

Location: `part3/part3_final_cleaned.ipynb`

This part brings together the datasets produced in Part 1 and Part 2.  
It loads the BLS time-series files and the population JSON from S3, cleans the data, and performs the required analytics.

The work is completed inside a Jupyter Notebook (`part3_final_cleaned.ipynb`), with an optional Python script (`transform_phase1_3.py`) that performs the same transformations outside the notebook.

---

## What the Notebook Does

### A. Load BLS Time-Series Data from S3
- Reads all PRS `.txt` files uploaded by Part 1.
- Combines them into a single dataframe.
- Standardizes column names, trims whitespace, and converts numeric fields.
- Ensures `value` and `year` are valid numeric types.

### B. Load Population JSON Data from S3
- Reads the output from `population_data.json` uploaded in Part 2.
- Converts it into a dataframe, selecting only the relevant fields (Nation, Year, Population).
- Cleans the `Year` column and casts it to integers.

### C. Compute Mean and Standard Deviation of Population (2013–2018)
The notebook:
- Filters the population dataframe to the interval [2013, 2018].
- Calculates:
  - Mean population across those years.
  - Standard deviation of population across those years.

These results satisfy the population analytics portion of the assignment.

### D. Find the Best Year per Series (Largest Sum of Quarterly Values)
Using the time-series dataframe:
- Group the dataset by `series_id` and `year`.
- Sum the `value` column across quarterly periods (`Q01`, `Q02`, `Q03`, `Q04`).
- Identify, for each `series_id`, the year with the highest total.
- Produce a report containing:
  - `series_id`
  - `best_year`
  - `summed_value`

This matches the example described in the assignment prompt.

### E. Join BLS and Population Data for a Specific Series
For:
- `series_id = PRS30006032`
- `period = Q01`

The notebook:
- Filters the BLS dataframe to the matching series and period.
- Joins the filtered table with the population dataframe on `year`.
- Produces a combined view containing:
  - series_id
  - year
  - period
  - value
  - population (for that year)

This final output completes the third component of the assignment.

---

## Running the Notebook

To launch the notebook:
```bash
cd part3
jupyter notebook bls_population_analytics.ipynb
```
**Note:**
Make sure your AWS credentials and S3 bucket configuration are correctly set in the root `.env` file before running, since the notebook loads data directly from S3.

---

## Results of analytics

Part 3 (The analytics) produces three key results:

1. Mean and standard deviation of US population for the years 2013–2018.
2. A complete table listing, for each BLS `series_id`, the year with the highest summed quarterly value.
3. A joined report for `PRS30006032` and `Q01`, including population for the same year.

These outputs complete the analytical portion of the Rearc Quest assignment.

----

# Part 4: Automated Data Pipeline with AWS CDK

The fourth part of the project moves everything into infrastructure that can run on its own no manual trigger and no manual verification, it completes the workflow by infact automating it.  
Instead of manually running the scripts from Part 1, Part 2 and Part 3, the AWS CDK stack deploys a fully-automated pipeline that:

1. Syncs BLS data and ingests population data each day.  
2. Writes all outputs into S3.  
3. Triggers an SQS queue on every new file.  
4. Processes each SQS message with an analytics Lambda that runs the same logic covered in Part 3.

Everything lives in `part4/` and deploys through the CDK. While Part 1 and Part 2 are for populating, Part 3 is for analytics this part of the project makes sure that those tasks were a one off.

---

## What the Infrastructure Contains

### 1. A Daily Ingestion Lambda
Location: `part4/lambda/ingestion_handler.py`

This Lambda bundles all of Part 1 and Part 2 into one job.  
When it runs, it does the following:

- Scrapes and mirrors the entire BLS PRS dataset into S3.  
- Pulls population data from the DataUSA API.  
- Uses MD5 hashing and ETag comparison to avoid redundant uploads.  
- Logs everything so you can see what changed on each run.

This Lambda is scheduled with an EventBridge rule to run once per day.

### 2. An S3 Bucket With Event Notifications
The stack creates a dedicated S3 bucket where all data is stored.

Any time a new file lands in a specific prefix (the BLS files or the population JSON), the bucket publishes an SQS message.  
This is wired using S3 → SQS notifications.

# Why Docker?

AWS CDK on my windows environment has been a mixed bag till now and I wanted to rectify the hit and miss approach so I looked elewhere, the issues were on the lines of packages fail to compile, Node vs Python tools conflict, and CDK sometimes installs Linux-only Lambda layers that Windows can’t build.
Docker solves all of that in one shot.

### What Docker gives us here

-> **Identical environment** to AWS Lambda (Linux-based)  
-> **No dependency issues** between Windows and Linux runtimes  
-> **Guaranteed reproducible CDK builds**  
-> **You never pollute your system Python or Node installations**  
-> **You avoid the classic “Lambda package built on Windows won’t run on Lambda runtime” problem**

All CDK commands happen *inside the container*, so they always behave exactly like AWS expects.

### In simple terms  
Docker keeps the dev environment clean, consistent, and predictable.  
And for Lambda projects, that’s huge.

The question now arises is that this workflow sounds fun but how do I really implement it and that is when I leaned to several documentation and public articles for navigating the right way through, few of the sources are:
#### A. AWS Official Docs  
**AWS CDK – Using Docker to bundle Lambda functions**  
https://docs.aws.amazon.com/cdk/v2/guide/asset_types.html#asset_types_docker

AWS themselves recommend Docker when building Lambda packages for Linux, especially when working on Windows or macOS.

#### B. AWS Containers Blog  
**“Building Lambda functions with container images”**  
https://aws.amazon.com/blogs/containers/building-lambda-functions-with-container-images/

This article explains why Docker is a safe, consistent build environment.

#### C. Official AWS GitHub Example (CDK + Docker)  
https://github.com/aws-samples/aws-cdk-examples/tree/master/python/lambda-bundling

Shows how Docker is used to ensure the Lambda environment matches AWS runtime.

**Note:** The process of Docker imaging and deployment came with its issues specially with the WSL2 configuration and IAM permissions and this is one of the major chunks where ChatGPT and Google Gemini were used for guidance.


### 3. An SQS Queue for Downstream Analytics
The queue receives a message every time ingestion is finished and a new file is written to S3.

The message includes:
- bucket name  
- object key  
- event type  

This is enough for the analytics Lambda to fetch the object and process it.

### 4. An Analytics Lambda
Location: `part4/lambda/analytics_handler.py`

This Lambda does the transformation and analytics you wrote in Part 3.  
Instead of creating notebooks, it simply:

- Loads the BLS files and population JSON from S3  
- Runs the same cleaning and transformations  
- Computes the Part 3 metrics  
- Logs the results (CloudWatch)

No notebooks or local environment required, everything runs inside AWS automatically.

### 5. The CDK Stack Itself
Location: `part4/cdk_stack.py`

The stack creates:

- S3 bucket  
- SQS queue  
- IAM roles for both Lambdas  
- Daily EventBridge rule  
- Two Lambda functions  
- S3 → SQS event notifications  

It connects all of these together so the flow is completely automated.

### 6. Optional Docker Setup
If you want a reproducible environment for CDK deployments, the repo includes:

- `Dockerfile`  
- `docker-compose.yml`  
- `docker-cdk.ps1`  

These aren’t required to deploy, but they make the setup fully containerized if needed.

---

## Deployment Workflow

To deploy from `part4/`:

```bash
cd part4
pip install -r requirements.txt
cdk bootstrap       # only once ever per AWS account
cdk deploy
```
Once deployed, AWS takes over:

1. Every day, the ingestion Lambda runs.  
2. It downloads the BLS files, checks hashes, updates them in S3, and fetches new population data.  
3. When it writes to S3, that write triggers an S3 event.  
4. S3 sends a message to SQS.  
5. SQS wakes up the analytics Lambda.  
6. The analytics Lambda runs the full Part 3 transformations and logs:  
   - mean and std dev of population (2013–2018),  
   - best year for each series,  
   - joined result for PRS30006032 + Q01.

And that’s it. A complete data pipeline, maintained by AWS, no manual intervention needed.

---

## How Part 4 Connects Back to Parts 1–3

Think of Part 4 as the production version of everything you built:

- **Part 1** → ingestion Lambda downloads and syncs BLS  
- **Part 2** → ingestion Lambda fetches population  
- **Part 3** → analytics Lambda runs the S3-driven analysis  
- **S3 + SQS** → event-driven orchestration  
- **AWS CDK** → everything deployed consistently with one command

You took three independent scripts and turned them into a real cloud-native data pipeline.

---

# AI Assistance Disclosure

I used AI as a support tool while building this project, but all core decisions, code design, debugging, and architecture were done by me. AI wasn’t a substitute for thinking; it was more like a second pair of eyes that helped me move faster and stay unblocked. As I truly believe it would be lost opportunity to not use AI specially today but the question rises in fair and right usage of AI models.

### How I used AI
Here’s the breakdown of where AI actually helped:

### 1. **Research acceleration**
Some parts of this project required switching mental contexts quickly (BLS syncing logic, S3 ETags, deterministic hashing, S3→SQS workflows, CDK best practices, Docker for CDK builds, WSL2 compatibility issues).  
Instead of digging through dozens of docs every time, I used AI to get a starting point and confirm whether my understanding matched what AWS expected.

### 2. **Debugging and rectifying mistakes**
Whenever I hit confusing behavior (S3 ETag mismatches, Pandas dtype issues, CDK bootstrapping errors, Docker networking, or WSL2 path mapping), I used AI to sanity-check my approach.  
It never wrote the fix for me — it told me *why* something was wrong so I could correct it myself.

Examples:
- Why my SQS visibility timeout needed to be higher than Lambda timeout
- Why CDK couldn’t load `aws_cdk_lib` until I set up the right Docker image
- Why deterministic JSON hashing must use sorted keys
- How to handle BLS `.txt` parsing edge cases

In other words, AI acted more like a verifier: “Yes, this logic is correct”, or “Actually this S3 ETag behavior is misleading, check multipart upload rules”.

### 3. **Refactoring + cleanup**
I wrote the functional code first. Then I used AI to help me trim repeated blocks, improve print statements, and make the CLI output cleaner.  
The ideas and logic stayed the same — it helped with readability.

### 4. **Documentation**
The README (especially Part 4) benefited the most from AI. I know the pipeline well, but AI helped me phrase the story clearly and cleanly without fluff.

### 5. **Docker + WSL2 troubleshooting**
This was the biggest lift where AI helped.  
Running AWS CDK with Python on Windows is notoriously annoying, and Docker was the cleanest way to avoid dependency pollution and version mismatch.

AI helped me:
- confirm WSL2 was enabled  
- confirm Ubuntu was running  
- understand why CDK must run inside Docker (to avoid Windows path/env issues)  
- understand the correct CDK bootstrap flow  
- fix mismatched Python versions inside the container  
- match Dockerfile, CDK version, and lambda build settings

Without that guidance, I’d have lost a lot more time fighting environment inconsistencies.

### What AI got wrong
- Sometimes it hallucinated AWS limits (for example claiming certain ETag rules apply universally, which is only true for single-part uploads).  
- It occasionally suggested CDK constructs that were deprecated.  
- It was overconfident about S3 notification constraints until I checked the AWS docs myself.  

I cross-checked everything against AWS documentation or by testing directly.

### What I did myself
- All code logic (scraping, hashing, ingestion, S3 sync, transformations, merging datasets)
- Complete refactors for deterministic hashing, file diffs, and S3 deletion logic
- Entire CDK stack structure (S3, SQS, Lambdas, permissions, IAM policies)
- Full debugging of deployment failures
- Architecture decisions (why two Lambdas, why S3→SQS→Lambda fanout, why Docker)
- Notebook analytics and validation of results

### Why I’m being explicit about this
The assignment encourages AI as a *reference* but expects the engineer to deeply understand the work.  
Every part of this project is something I can explain in detail because the reasoning and decisions were mine. AI just made the research smoother and the development loop tighter.



