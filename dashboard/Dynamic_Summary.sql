SELECT
  CASE
    WHEN COUNT(DISTINCT d.restaurant_id) = 1 THEN
      CONCAT(
        'Branch: ', MAX(COALESCE(r.name, d.restaurant_id)),
        ' | Order: ', MAX(d.sarima_order),
        ' | Holdout WAPE: ', ROUND(MAX(d.holdout_mape_pct), 1), '%',
        ' | Residuals: ', MAX(d.residuals_pass),
        ' | Model used: ', CASE WHEN MAX(d.status) = 'OK' THEN 'ARIMAX' ELSE 'Seasonal-median fallback' END
      )
    ELSE
      CONCAT(
        'Chain-Wide Status (as of ', MAX(d.model_run_date), ') | Avg Holdout WAPE: ',
        ROUND(AVG(d.holdout_mape_pct), 1), '%',
        ' | Passing Residual Diagnostics: ',
        SUM(CASE WHEN d.residuals_pass = 'PASS' THEN 1 ELSE 0 END), '/', COUNT(*), ' Branches',
        ' | On Fallback: ',
        SUM(CASE WHEN d.status = 'OK_FALLBACK_SEASONAL_MEDIAN' THEN 1 ELSE 0 END), '/', COUNT(*), ' Branches'
      )
  END AS diagnostics_footer
FROM zaferan_sofreh.platinum.daily_revenue_forecast_diagnostics d
LEFT JOIN zaferan_sofreh.silver.dim_restaurant r
       ON d.restaurant_id = r.restaurant_id;
 