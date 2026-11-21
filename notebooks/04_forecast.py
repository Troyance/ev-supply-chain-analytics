# Databricks notebook source
# MAGIC %md
# MAGIC # forecasting
# MAGIC
# MAGIC Two M5-style LightGBM models:
# MAGIC
# MAGIC - **vehicle builds** per plant × model (4 series). Part demand at the plants comes from exploding this
# MAGIC   through the BOM later — nobody forecasts plant parts directly.
# MAGIC - **service parts** demand at the DC (~280 lumpy, intermittent series). Tweedie objective, like the M5 winners.
# MAGIC
# MAGIC How it's set up:
# MAGIC
# MAGIC - one global model per type, not one per series
# MAGIC - direct multi-horizon: each training row is (origin week, horizon h) → actual h weeks later, h = 1..13
# MAGIC - targets are normalized so trees don't have to extrapolate: builds as a ratio to the last 4 weeks,
# MAGIC   service demand as a rate per 1k vehicles in the field
# MAGIC - backtest on the last 26 origins vs a naive baseline, intervals from backtest errors
# MAGIC - everything gets logged to MLflow, the models get registered in Unity Catalog, and forecasts are
# MAGIC   appended to `fct_forecast` under a `run_id`

# COMMAND ----------

import json, uuid, datetime as dt
import numpy as np
import pandas as pd
import lightgbm as lgb
import mlflow
from pyspark.sql import functions as F

SCHEMA = "workspace.ev_supply_chain"
H = 13
VAL_ORIGINS = 26

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{user}/ev-supply-chain-forecasts")

builds = spark.table(f"{SCHEMA}.fct_vehicle_builds").toPandas()
service = spark.table(f"{SCHEMA}.fct_service_demand").toPandas()
part_cat = spark.table(f"{SCHEMA}.dim_part_planning").select("part_id", "component_category").toPandas()
for d in (builds, service):
    d["week_start"] = pd.to_datetime(d["week_start"])

last_week = builds["week_start"].max()
val_start = last_week - pd.Timedelta(weeks=H + VAL_ORIGINS - 1)
print("history ends", last_week.date(), "| backtest origins from", val_start.date())

# vehicles in the field, and how fast that's growing
total_builds = builds.groupby("week_start")["units"].sum().sort_index()
fleet = total_builds.cumsum()
build_rate = total_builds.rolling(4, min_periods=1).mean()

# COMMAND ----------

# MAGIC %md
# MAGIC ## features
# MAGIC
# MAGIC Everything is computed as of the origin week, so there's no peeking at the future.

# COMMAND ----------

def make_rows(df, keys, kind):
    df = df.sort_values(keys + ["week_start"]).reset_index(drop=True)
    g = df.groupby(keys, sort=False)["y"]
    df["mean4"] = g.transform(lambda s: s.rolling(4, min_periods=1).mean())
    df["mean13"] = g.transform(lambda s: s.rolling(13, min_periods=1).mean())
    df["mean52"] = g.transform(lambda s: s.rolling(52, min_periods=1).mean())
    df["std13"] = g.transform(lambda s: s.rolling(13, min_periods=2).std()).fillna(0)
    df["age"] = g.cumcount()

    if kind == "builds":
        hist_scale = df["mean4"] + 1
    else:
        hist_scale = df["week_start"].map(fleet) / 1000

    out = []
    for h in range(1, H + 1):
        d = df.copy()
        d["h"] = h
        d["target_week"] = d["week_start"] + pd.Timedelta(weeks=h)
        d["y_target"] = g.shift(-h)
        d["y_last_year"] = g.shift(52 - h)
        if kind == "builds":
            d["scale"] = hist_scale
        else:  # project the fleet forward h weeks at the current build rate
            d["scale"] = (d["week_start"].map(fleet) + h * d["week_start"].map(build_rate)) / 1000
        for c in ["y", "mean4", "mean13", "mean52", "std13", "y_last_year"]:
            d[f"n_{c}"] = d[c] / hist_scale
        d["target_woy"] = d["target_week"].dt.isocalendar().week.astype(int)
        d["target_month"] = d["target_week"].dt.month
        out.append(d)

    rows = pd.concat(out, ignore_index=True)
    rows = rows[rows["age"] >= 3]  # need a few weeks of history before forecasting
    rows["y_norm"] = rows["y_target"] / rows["scale"]
    return rows

BASE_FEATURES = ["h", "age", "target_woy", "target_month", "n_y", "n_mean4", "n_mean13",
                 "n_mean52", "n_std13", "n_y_last_year"]

# builds: one series per plant × model
b = builds.rename(columns={"units": "y"})
b["series_code"] = (b["site_id"] + "-" + b["model_id"]).astype("category").cat.codes
build_rows = make_rows(b, ["site_id", "model_id"], "builds")

# service: one series per part
s = service.rename(columns={"units": "y"}).merge(part_cat, on="part_id")
s["category_code"] = s["component_category"].astype("category").cat.codes
service_rows = make_rows(s, ["site_id", "part_id"], "service")

print(len(build_rows), "build rows |", len(service_rows), "service rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## train, backtest, register
# MAGIC
# MAGIC 1. find the number of trees using an inner holdout (the 13 weeks before the backtest window)
# MAGIC 2. refit on everything before the backtest window, score the backtest
# MAGIC 3. refit on all history and forecast the next 13 weeks from the latest week

# COMMAND ----------

def wape(actual, pred):
    return float(np.abs(actual - pred).sum() / max(actual.sum(), 1e-9))

def bias(actual, pred):
    return float((pred - actual).sum() / max(actual.sum(), 1e-9))

def fit(train, features, params, rounds):
    ds = lgb.Dataset(train[features], train["y_norm"], categorical_feature=[f for f in features if f.endswith("_code")])
    return lgb.train(params, ds, num_boost_round=rounds)

def run_forecast(rows, features, params, kind, model_name, baseline_col):
    labeled = rows[rows["y_target"].notna()]
    train = labeled[labeled["target_week"] <= val_start]
    backtest = labeled[labeled["week_start"] >= val_start].copy()

    # 1. pick number of trees on an inner holdout
    inner_cut = val_start - pd.Timedelta(weeks=H)
    inner_tr, inner_va = train[train["target_week"] <= inner_cut], train[train["week_start"] > inner_cut]
    cats = [f for f in features if f.endswith("_code")]
    booster = lgb.train(
        params,
        lgb.Dataset(inner_tr[features], inner_tr["y_norm"], categorical_feature=cats),
        num_boost_round=2000,
        valid_sets=[lgb.Dataset(inner_va[features], inner_va["y_norm"], categorical_feature=cats)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    best_iter = booster.best_iteration or 200

    # 2. backtest
    model = fit(train, features, params, best_iter)
    backtest["p50"] = np.clip(model.predict(backtest[features]), 0, None) * backtest["scale"]
    backtest["naive"] = backtest[baseline_col]

    # intervals: spread of actual/forecast in the backtest, by horizon
    ok = backtest[backtest["p50"] > 0.5]
    ratio = (ok["y_target"] / ok["p50"]).groupby(ok["h"])
    q10, q90 = ratio.quantile(0.1), ratio.quantile(0.9)
    backtest["p10"] = backtest["p50"] * backtest["h"].map(q10).fillna(1)
    backtest["p90"] = backtest["p50"] * backtest["h"].map(q90).fillna(1)

    metrics = {
        "wape": wape(backtest["y_target"], backtest["p50"]),
        "bias": bias(backtest["y_target"], backtest["p50"]),
        "naive_wape": wape(backtest["y_target"], backtest["naive"]),
        "interval_coverage_p10_p90": float(((backtest["y_target"] >= backtest["p10"]) &
                                            (backtest["y_target"] <= backtest["p90"])).mean()),
        "best_iteration": best_iter,
    }
    for h in (1, 4, 13):
        hb = backtest[backtest["h"] == h]
        metrics[f"wape_h{h}"] = wape(hb["y_target"], hb["p50"])

    # 3. final model on all history, forecast forward from the last week
    final = fit(labeled, features, params, best_iter)
    future = rows[rows["week_start"] == last_week].copy()
    future["p50"] = np.clip(final.predict(future[features]), 0, None) * future["scale"]
    future["p10"] = future["p50"] * future["h"].map(q10).fillna(1)
    future["p90"] = future["p50"] * future["h"].map(q90).fillna(1)

    with mlflow.start_run(run_name=f"{kind}-{dt.date.today()}") as run:
        mlflow.log_params({**params, "horizon_weeks": H, "backtest_origins": VAL_ORIGINS,
                           "features": ",".join(features), "history_end": str(last_week.date())})
        mlflow.log_metrics(metrics)
        imp = dict(zip(features, final.feature_importance("gain").round(1).tolist()))
        mlflow.log_dict(imp, "feature_importance.json")
        info = mlflow.lightgbm.log_model(
            final, "model",
            registered_model_name=f"{SCHEMA}.{model_name}",
            input_example=future[features].head(5),
        )
        version = getattr(info, "registered_model_version", None)
        mlflow_run_id = run.info.run_id

    return backtest, future, metrics, imp, str(version) if version else None, mlflow_run_id

build_params = dict(objective="regression", learning_rate=0.05, num_leaves=15, min_data_in_leaf=20,
                    feature_fraction=0.9, verbose=-1, seed=7)
service_params = dict(objective="tweedie", tweedie_variance_power=1.2, learning_rate=0.05, num_leaves=63,
                      min_data_in_leaf=100, feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1,
                      verbose=-1, seed=7)

b_bt, b_fut, b_metrics, b_imp, b_ver, b_run = run_forecast(
    build_rows, BASE_FEATURES + ["series_code"], build_params, "vehicle_builds", "vehicle_build_forecaster", "mean4")
s_bt, s_fut, s_metrics, s_imp, s_ver, s_run = run_forecast(
    service_rows, BASE_FEATURES + ["category_code"], service_params, "service_parts", "service_parts_forecaster", "mean13")

# COMMAND ----------

# MAGIC %md
# MAGIC ## write to the forecast data model
# MAGIC
# MAGIC Forecasts are **append-only** and keyed by `run_id`. Never overwrite a forecast — keeping every run is what
# MAGIC lets you measure accuracy later, compare models, and answer "what did we think in October?"
# MAGIC
# MAGIC - `forecast_run` – one row per run: what model, which version, trained through when, how it backtested
# MAGIC - `fct_forecast` – one row per run × series × target week, with p10 / p50 / p90
# MAGIC
# MAGIC The naive baseline is written too (as its own model), so accuracy can be compared side by side.

# COMMAND ----------

now = dt.datetime.utcnow()

FORECAST_SCHEMA = """run_id string, forecast_type string, model_name string, origin_week date, target_week date,
                     horizon_weeks int, site_id string, model_id string, part_id bigint,
                     forecast_p10 double, forecast_p50 double, forecast_p90 double"""
RUN_SCHEMA = """run_id string, run_type string, forecast_type string, model_name string, model_version string,
                mlflow_run_id string, history_end_week date, origin_week_start date, origin_week_end date,
                horizon_weeks int, wape double, bias double, created_at timestamp"""

def forecast_frame(df, run_id, kind, model_name, p50_col="p50", with_interval=True):
    return pd.DataFrame({
        "run_id": run_id, "forecast_type": kind, "model_name": model_name,
        "origin_week": df["week_start"].dt.date, "target_week": df["target_week"].dt.date,
        "horizon_weeks": df["h"].astype(int), "site_id": df["site_id"],
        "model_id": df["model_id"] if "model_id" in df else None,
        "part_id": df["part_id"].astype("Int64") if "part_id" in df else None,
        "forecast_p10": df["p10"] if with_interval else np.nan,
        "forecast_p50": df[p50_col].astype(float),
        "forecast_p90": df["p90"] if with_interval else np.nan,
    })

forecasts, runs = [], []
for kind, bt, fut, m, ver, mlrun, model_name in [
    ("vehicle_builds", b_bt, b_fut, b_metrics, b_ver, b_run, "vehicle_build_forecaster"),
    ("service_parts", s_bt, s_fut, s_metrics, s_ver, s_run, "service_parts_forecaster"),
]:
    bt_id, naive_id, prod_id = (str(uuid.uuid4()) for _ in range(3))
    forecasts += [forecast_frame(bt, bt_id, kind, model_name),
                  forecast_frame(bt, naive_id, kind, "naive_mean", "naive", with_interval=False),
                  forecast_frame(fut, prod_id, kind, model_name)]
    o_start, o_end = bt["week_start"].min().date(), bt["week_start"].max().date()
    runs += [
        (bt_id, "backtest", kind, model_name, ver, mlrun, last_week.date(), o_start, o_end, H, m["wape"], m["bias"], now),
        (naive_id, "backtest", kind, "naive_mean", None, None, last_week.date(), o_start, o_end, H,
         m["naive_wape"], bias(bt["y_target"], bt["naive"]), now),
        (prod_id, "production", kind, model_name, ver, mlrun, last_week.date(),
         last_week.date(), last_week.date(), H, None, None, now),
    ]

fc = pd.concat(forecasts, ignore_index=True)
fc["model_id"] = fc["model_id"].astype(object).where(fc["model_id"].notna(), None)
fc["part_id"] = fc["part_id"].astype(object).where(fc["part_id"].notna(), None)
fc = fc.replace({np.nan: None})

spark.createDataFrame(fc, FORECAST_SCHEMA).write.mode("append").saveAsTable(f"{SCHEMA}.fct_forecast")
spark.createDataFrame(pd.DataFrame(runs), RUN_SCHEMA).write.mode("append").saveAsTable(f"{SCHEMA}.forecast_run")

# COMMAND ----------

top = lambda imp: dict(sorted(imp.items(), key=lambda kv: -kv[1])[:5])
next13 = b_fut.groupby(["site_id", "model_id"])["p50"].sum().round().astype(int)
result = {
    "history_end": str(last_week.date()),
    "vehicle_builds": {**{k: round(v, 3) for k, v in b_metrics.items()}, "uc_version": b_ver,
                       "top_features": top(b_imp),
                       "next_13wk_units": {f"{k[0]}-{k[1]}": int(v) for k, v in next13.items()}},
    "service_parts": {**{k: round(v, 3) for k, v in s_metrics.items()}, "uc_version": s_ver,
                      "top_features": top(s_imp), "series": int(s_fut["part_id"].nunique())},
    "forecast_rows_written": len(fc),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
