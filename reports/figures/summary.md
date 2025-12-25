# Week 2 Summary — ETL + EDA Pipeline

## Key Findings
1.  **Market Dominance:** Saudi Arabia (SA) accounts for the majority of total revenue (~80%), significantly outperforming the UAE (AE).
2.  **High Refund Risk in UAE:** While SA has higher volume, the UAE (AE) shows a noticeably higher **refund rate** (~20.8%) compared to SA (~11.8%). This suggests potential issues with logistics or product quality specific to the UAE market.
3.  **Order Value Distribution:** The median order value is significantly lower than the mean. The distribution is right-skewed, meaning revenue is driven by a high volume of small orders rather than a few large ones.
4.  **Weekly Trend:** Sales revenue remained relatively stable between 300 and 500 per week, peaking in mid-January. The sharp drop in the final week is attributed to partial data collection, not a genuine drop in performance.

## Definitions
*   **Revenue:** Sum of the `amount` column for valid orders.
*   **Refund Rate:** Calculated as `count(refunds) / count(total_orders)`, where status is normalized to "refund".
*   **Outliers:** Orders with an `amount` greater than `1.5 * IQR` were flagged. For visualization purposes, these were capped (winsorized) to the 99th percentile.

## Data Quality Caveats
*   **Join Integrity:** We achieved a **100% match rate** when joining orders with users, indicating high data integrity between the two source systems.
*   **Missing Data:** A small percentage of `created_at` timestamps were invalid strings and were parsed as `NaT` (Not a Time).
*   **Invalid Amounts:** The raw `amount` column contained non-numeric values (e.g., "not_a_number"), which were coerced to `NaN` during the cleaning process.
*   **Time Window:** The analysis covers data from Dec 2025 to Jan 2026. The final week appears incomplete.

## Next Steps
1.  **Business Investigation:** Deep dive into UAE operations to understand the root cause of the 20% refund rate.
2.  **Data Engineering:** Automate the ingestion of weekly CSV files to keep this report up-to-date.
3.  **Analysis:** Segment the data by "Product Category" (if available in the future) to see if specific items drive the high refund rates.