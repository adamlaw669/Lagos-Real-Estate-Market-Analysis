# Lagos Real Estate Market Analysis

This project explores the Lagos residential market from raw listings through to a cleaned dataset, exploratory analysis, and a written report. The goal is simple: understand how price, location, property type, and listing activity work together in the market.

## Project Structure

- `src/scraper.py` — Selenium scraper used to collect the listings
- `notebooks/01_data_acquisition.ipynb` — acquisition workflow and notes
- `notebooks/02_data_cleaning.ipynb` — cleaning, standardization, and prep
- `notebooks/03_exploratory_data_analysis.ipynb` — the main EDA notebook
- `reports/lagos_real_estate_eda_report.md` — article-style report generated from the analysis
- `reports/figures/` — supporting charts created during the report build

## What The Analysis Covers

- price distribution and skewness
- location and district concentration
- property kind and bedroom/bathroom patterns
- market segmentation into low, mid, high, and luxury bands
- agent concentration and listing activity
- correlations between the main numeric features
- luxury-market behavior in the top 1% of listings

## Data

- `data/raw/` — scraped raw CSV files
- `data/processed/lagos_real_estate_market_data_cleaned.csv` — cleaned dataset used for analysis
- Published and reusable on [Kaggle](https://www.kaggle.com/datasets/adamlawal/lagos-state-real-estate-data-cw)

## Setup

Install the project dependencies with:

```bash
pip install -r requirements.txt
```

## How To Run

1. Open the notebooks in order, starting with acquisition and cleaning.
2. Run `notebooks/03_exploratory_data_analysis.ipynb` or the narrative copy version for the full EDA.
3. Open `reports/lagos_real_estate_eda_report.md` for the written summary version of the analysis.

## Notes

- The report folder contains the polished write-up and the supporting figures.
- If you rerun the report generation, the figures in `reports/figures/` will be recreated automatically.
- The detailed report is available on [Medium](https://adamlawal.medium.com/exploratory-data-analysis-on-alx-nigeria-learner-outcomes-ae47cf46a96d)
