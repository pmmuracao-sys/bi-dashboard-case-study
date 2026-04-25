# bi-dashboard-case-study
This project shows a simple end-to-end workflow: from raw campaign data to a final dashboard used to analyze performance across platforms.

The focus is not just on visuals, but on how the data is structured and how it supports decision-making.

---

## Objective

Analyze marketing campaigns across platforms (Meta, TikTok, DV360) to understand:

- Which platforms generate more leads
- How much each lead costs
- Which campaigns are more efficient

---

## Process

1. Load raw campaign data
2. Clean and standardize column names
3. Convert data types (dates, numeric fields)
4. Build a structured dataset for reporting
5. Create a dashboard in Power BI with key metrics

---

## Key Metrics

- **Leads**: total conversions
- **Spend**: total investment
- **CPA (Cost per Acquisition)** = Spend / Leads  
- **CPC (Cost per Click)** = Spend / Clicks  
- **CTR (Click-through Rate)** = Clicks / Impressions  

---

## Example Insights

- Meta generates the highest number of leads, but not always at the lowest cost  
- TikTok shows solid performance with competitive CPC  
- DV360 contributes less to total leads and may require budget review  

---

## Project Structure

/data
/raw
/processed
/scripts
transform_data.py
/dashboard
Marketing_Test.pbix
/portfolio


---

## Tools Used

- Python (pandas)
- Power BI


## Notes

This is a sample project using simulated data. The goal is to demonstrate the workflow from data preparation to final reporting.

## Portfolio Summary

You can view a summary of the dashboards and key insights here:

- [Portfolio PDF](./Pablo_Muracao_Portfolio_v2.pdf)
