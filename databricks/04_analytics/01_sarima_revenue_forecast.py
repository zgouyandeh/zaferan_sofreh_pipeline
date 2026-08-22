"""
databricks/04_analytics/01_sarima_revenue_forecast.py
------------------------------------------------------
Fits one ARIMAX model PER restaurant in parallel across Spark workers via
a Pandas UDF (`groupBy().applyInPandas`), writing 14-day forecasts with
80% prediction intervals and operational ceiling caps to Platinum schema.

Refined Fallback Logic:
1. Benchmark against a Seasonal-Naive baseline (t - 7 days).
2. Fallback triggers ONLY if ARIMAX fails Ljung-Box residual diagnostics 
   (Ljung-Box p <= 0.05) OR fails to beat a seasonal-naive ("repeat last 
   week's actuals") benchmark on holdout WAPE (Skill score <= 0%).
   An earlier version used a flat 25% WAPE cutoff -- that penalizes every 
   restaurant equally regardless of how inherently noisy its demand genuinely is.
   Benchmarking against a naive forecast instead asks the right question: 
   does ARIMAX add value over the trivial baseline, not "is WAPE below an 
   arbitrary constant." This mirrors the spirit of MASE (Mean Absolute Scaled 
   Error, Hyndman & Koehler 2006).

Inputs : zaferan_sofreh.gold_v2.daily_restaurant_performance
Outputs: zaferan_sofreh.platinum.daily_revenue_forecast_by_restaurant
         zaferan_sofreh.platinum.daily_revenue_forecast_diagnostics
"""
from __future__ import annotations

import itertools
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.statespace.sarimax import SARIMAX

SEASONAL_PERIOD = 7
FORECAST_HORIZON_DAYS = 14
HOLDOUT_DAYS = 14
MIN_HISTORY_DAYS = 90
CI_ALPHA = 0.20  # 80% prediction interval
FALLBACK_LJUNG_BOX_THRESHOLD = 0.05

# No absolute WAPE cutoff -- the fallback decision is relative to a
# seasonal-naive benchmark computed per restaurant (see
# _seasonal_naive_forecast below), not a flat constant every branch is
# held to regardless of how noisy its demand genuinely is.

OUTPUT_SCHEMA = StructType(
    [
        StructField("restaurant_id", StringType(), True),
        StructField("record_type", StringType(), True),   # 'forecast' | 'diagnostics'
        StructField("forecast_date", DateType(), True),
        StructField("predicted_revenue", DoubleType(), True),
        StructField("lower_ci", DoubleType(), True),
        StructField("upper_ci", DoubleType(), True),
        StructField("sarima_order", StringType(), True),
        StructField("sarima_seasonal_order", StringType(), True),
        StructField("aic", DoubleType(), True),
        StructField("aicc", DoubleType(), True),
        StructField("ljung_box_pvalue", DoubleType(), True),
        StructField("residuals_pass", StringType(), True),
        StructField("holdout_mape_pct", DoubleType(), True),    # actually WAPE % -- see module docstring
        StructField("naive_wape_pct", DoubleType(), True),      # seasonal-naive benchmark WAPE %
        StructField("skill_vs_naive_pct", DoubleType(), True),  # 100 * (1 - arimax_wape/naive_wape); >0 means ARIMAX beats naive
        StructField("n_obs_used", IntegerType(), True),
        StructField("status", StringType(), True),
    ]
)


def _aicc(aic: float, n_obs: int, n_params: int) -> float:
    denom = n_obs - n_params - 1
    if denom <= 0:
        return float("inf")
    return aic + (2 * n_params * (n_params + 1)) / denom


def _determine_d(series: pd.Series, max_d: int = 1, alpha: float = 0.05) -> int:
    working = series.copy()
    for d in range(max_d + 1):
        try:
            _, p_value, *_ = adfuller(working.dropna(), autolag="AIC")
        except Exception:
            return d
        if p_value < alpha:
            return d
        working = working.diff()
    return max_d


def _build_exog(date_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Builds exogenous feature matrix capturing Iranian calendar weekends (Thu/Fri)."""
    exog = pd.DataFrame(index=date_index)
    exog["is_thursday"] = (date_index.dayofweek == 3).astype(float)
    exog["is_friday"] = (date_index.dayofweek == 4).astype(float)
    return exog


def _select_and_fit(series: pd.Series, exog: pd.DataFrame):
    d = _determine_d(series)
    n_obs = len(series)

    best = None
    for p, q, P, Q, D in itertools.product(range(3), range(3), range(2), range(2), range(2)):
        if p == q == P == Q == 0:
            continue
        try:
            model = SARIMAX(
                series,
                exog=exog,
                order=(p, d, q),
                seasonal_order=(P, D, Q, SEASONAL_PERIOD),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit(disp=False)
        except Exception:
            continue

        n_params = len(fit.params)
        aicc = _aicc(fit.aic, n_obs, n_params)
        if best is None or aicc < best[3]:
            best = ((p, d, q), (P, D, Q, SEASONAL_PERIOD), fit.aic, aicc, fit)

    return best


def _seasonal_naive_forecast(
    train_actual: pd.Series, horizon: int, season: int = SEASONAL_PERIOD
) -> np.ndarray:
    """
    "Repeat last week's actual values" -- the cheapest defensible baseline
    for a weekly-seasonal series. Tiles the last observed full season to
    cover the forecast horizon. Any model that can't beat this on holdout
    WAPE isn't earning its complexity, regardless of how good its
    residual diagnostics look in isolation.
    """
    last_season = train_actual.iloc[-season:].values
    reps = int(np.ceil(horizon / season))
    return np.tile(last_season, reps)[:horizon]


def forecast_restaurant_revenue(pdf: pd.DataFrame) -> pd.DataFrame:
    restaurant_id = pdf["restaurant_id"].iloc[0]

    def _diag_row(status: str, n_obs: int = 0) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "restaurant_id": restaurant_id,
                    "record_type": "diagnostics",
                    "forecast_date": None,
                    "predicted_revenue": None,
                    "lower_ci": None,
                    "upper_ci": None,
                    "sarima_order": None,
                    "sarima_seasonal_order": None,
                    "aic": None,
                    "aicc": None,
                    "ljung_box_pvalue": None,
                    "residuals_pass": None,
                    "holdout_mape_pct": None,
                    "naive_wape_pct": None,
                    "skill_vs_naive_pct": None,
                    "n_obs_used": n_obs,
                    "status": status,
                }
            ]
        )

    pdf = pdf.copy()
    pdf["activity_date"] = pd.to_datetime(pdf["activity_date"])
    pdf["daily_revenue"] = pdf["daily_revenue"].astype("float64")

    ts = (
        pdf.groupby("activity_date")["daily_revenue"]
        .sum()
        .sort_index()
        .asfreq("D", fill_value=0.0)
    )

    if len(ts) < MIN_HISTORY_DAYS:
        return _diag_row(f"SKIPPED_INSUFFICIENT_HISTORY (n={len(ts)} < {MIN_HISTORY_DAYS})", len(ts))

    operational_cap = float(ts.quantile(0.99) * 1.25)
    ts_log = np.log1p(ts)

    exog_full = _build_exog(ts.index)

    train_log, holdout_log = ts_log.iloc[:-HOLDOUT_DAYS], ts_log.iloc[-HOLDOUT_DAYS:]
    exog_train, exog_holdout = exog_full.iloc[:-HOLDOUT_DAYS], exog_full.iloc[-HOLDOUT_DAYS:]
    holdout_actual = ts.iloc[-HOLDOUT_DAYS:]

    try:
        best = _select_and_fit(train_log, exog_train)
        if best is None:
            return _diag_row("FIT_FAILED_NO_CONVERGENCE", len(ts))
        sarima_order, sarima_seasonal_order, aic, aicc, fit_train = best

        # 1. ARIMAX Holdout WAPE
        holdout_forecast_log = fit_train.get_forecast(
            steps=HOLDOUT_DAYS, exog=exog_holdout
        ).predicted_mean
        holdout_forecast = np.expm1(holdout_forecast_log.values)

        sum_actual = np.sum(holdout_actual.values)
        if sum_actual > 0:
            mape_pct = float(np.sum(np.abs(holdout_forecast - holdout_actual.values)) / sum_actual * 100)
            naive_forecast = _seasonal_naive_forecast(ts.iloc[:-HOLDOUT_DAYS], HOLDOUT_DAYS)
            naive_wape_pct = float(np.sum(np.abs(naive_forecast - holdout_actual.values)) / sum_actual * 100)
            skill_vs_naive_pct = (
                float((1 - mape_pct / naive_wape_pct) * 100) if naive_wape_pct > 0 else float("nan")
            )
        else:
            mape_pct = float("nan")
            naive_wape_pct = float("nan")
            skill_vs_naive_pct = float("nan")

        # Refit ARIMAX model on full historical series
        try:
            final_model = SARIMAX(
                ts_log,
                exog=exog_full,
                order=sarima_order,
                seasonal_order=sarima_seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            final_fit = final_model.fit(disp=False)
        except Exception:
            final_fit = fit_train

        # Residual diagnostics
        lb_lags = min(10, max(1, len(ts) // 5))
        std_errors = pd.Series(final_fit.standardized_forecasts_error[0]).dropna()
        lb = acorr_ljungbox(std_errors, lags=[lb_lags], return_df=True)
        ljung_box_p = float(lb["lb_pvalue"].iloc[0])
        residuals_pass = "PASS" if ljung_box_p > 0.05 else "FAIL"

        # Generate future forecast
        future_dates = pd.date_range(
            start=ts.index[-1] + pd.Timedelta(days=1),
            periods=FORECAST_HORIZON_DAYS,
            freq="D",
        )
        exog_future = _build_exog(future_dates)

        fc = final_fit.get_forecast(steps=FORECAST_HORIZON_DAYS, exog=exog_future)
        summary = fc.summary_frame(alpha=CI_ALPHA)

        predicted_revenue = np.expm1(summary["mean"]).clip(lower=0, upper=operational_cap)
        lower_ci = np.expm1(summary["mean_ci_lower"]).clip(lower=0)
        upper_ci = np.expm1(summary["mean_ci_upper"]).clip(lower=0, upper=operational_cap)

        # ----------------------------------------------------------------------
        # Relative Fallback Decision: ARIMAX must both pass its own residual 
        # diagnostic AND beat the naive benchmark -- passing diagnostics 
        # alone isn't sufficient if a nearly-free baseline forecasts just as 
        # well or better.
        # ----------------------------------------------------------------------
        beats_naive = (
            not np.isnan(skill_vs_naive_pct) and skill_vs_naive_pct > 0
        )
        status_flag = "OK"
        if ljung_box_p <= FALLBACK_LJUNG_BOX_THRESHOLD or not beats_naive:
            status_flag = "OK_FALLBACK_SEASONAL_MEDIAN"
            recent_28 = ts.tail(28)
            dow_medians = recent_28.groupby(recent_28.index.dayofweek).median()
            dow_stds = recent_28.groupby(recent_28.index.dayofweek).std().fillna(0)

            fallback_preds = np.array([dow_medians.get(d.dayofweek, ts.median()) for d in future_dates])
            fallback_stds = np.array([dow_stds.get(d.dayofweek, ts.std()) for d in future_dates])

            predicted_revenue = pd.Series(fallback_preds, index=future_dates).clip(lower=0, upper=operational_cap)
            lower_ci = pd.Series(fallback_preds - 1.28 * fallback_stds, index=future_dates).clip(lower=0)
            upper_ci = pd.Series(fallback_preds + 1.28 * fallback_stds, index=future_dates).clip(lower=0, upper=operational_cap)

        forecast_rows = pd.DataFrame(
            {
                "restaurant_id": restaurant_id,
                "record_type": "forecast",
                "forecast_date": future_dates.date,
                "predicted_revenue": predicted_revenue.values,
                "lower_ci": lower_ci.values,
                "upper_ci": upper_ci.values,
                "sarima_order": str(sarima_order),
                "sarima_seasonal_order": str(sarima_seasonal_order),
                "aic": aic,
                "aicc": aicc,
                "ljung_box_pvalue": ljung_box_p,
                "residuals_pass": residuals_pass,
                "holdout_mape_pct": mape_pct,
                "naive_wape_pct": naive_wape_pct,
                "skill_vs_naive_pct": skill_vs_naive_pct,
                "n_obs_used": len(ts),
                "status": status_flag,
            }
        )

        diag_row = _diag_row(status_flag, len(ts))
        diag_row.loc[0, [
            "sarima_order", "sarima_seasonal_order", "aic", "aicc",
            "ljung_box_pvalue", "residuals_pass", "holdout_mape_pct",
            "naive_wape_pct", "skill_vs_naive_pct",
        ]] = [
            str(sarima_order), str(sarima_seasonal_order), aic, aicc,
            ljung_box_p, residuals_pass, mape_pct,
            naive_wape_pct, skill_vs_naive_pct,
        ]

        return pd.concat([forecast_rows, diag_row], ignore_index=True)

    except Exception as exc:
        return _diag_row(f"FIT_FAILED: {exc}", len(ts))


# ==============================================================================
# Spark execution & materialization into Platinum
# ==============================================================================

gold_perf_df = (
    spark.table("zaferan_sofreh.gold_v2.daily_restaurant_performance")
    .select(
        "restaurant_id",
        "activity_date",
        F.col("daily_revenue").cast("double").alias("daily_revenue"),
    )
)

results_df = (
    gold_perf_df.groupBy("restaurant_id")
    .applyInPandas(forecast_restaurant_revenue, schema=OUTPUT_SCHEMA)
    .withColumn("model_run_date", F.current_date())
)

# Materialize Forecasts
(
    results_df.filter(F.col("record_type") == "forecast")
    .withColumn(
        "forecast_method",
        F.when(F.col("status") == "OK", F.lit("ARIMAX"))
         .otherwise(F.lit("SEASONAL_MEDIAN_FALLBACK")),
    )
    .drop(
        "record_type", "sarima_order", "sarima_seasonal_order", "aic", "aicc",
        "ljung_box_pvalue", "residuals_pass", "holdout_mape_pct", "naive_wape_pct",
        "skill_vs_naive_pct", "status"
    )
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("zaferan_sofreh.platinum.daily_revenue_forecast_by_restaurant")
)

# Materialize Diagnostics
(
    results_df.filter(F.col("record_type") == "diagnostics")
    .drop("record_type", "forecast_date", "predicted_revenue", "lower_ci", "upper_ci")
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("zaferan_sofreh.platinum.daily_revenue_forecast_diagnostics")
)