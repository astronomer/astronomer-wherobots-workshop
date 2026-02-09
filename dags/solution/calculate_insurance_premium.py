from airflow.sdk import dag, task, chain, Asset
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_RUNTIME = Runtime.TINY
_REGION = Region.AWS_US_WEST_2
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "workshoptestnotebook")


@dag(
    schedule=[Asset("hail_comparison_ready")],
    tags=["Human-in-the-loop"],
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql"],
)
def insurance_premium_adjustment():

    _fetch_information_risk_comparison = WherobotsSqlOperator(
        task_id="fetch_information_risk_comparison",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/insurance_premium_adjustment/fetch_information_risk_comparison.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
        do_xcom_push=True,
    )

    @task
    def calculate_insurance_premium(risk_df, **context):
        us_postcode = context["var"]["value"].get("us_postcode")
        
        county = risk_df[risk_df["area_type"] == "county"].iloc[0]
        state = risk_df[risk_df["area_type"] == "state"].iloc[0]
        
        print(f"\n{'Metric':<20} {'County':<15} {'State':<15}")
        print("=" * 50)
        for col in risk_df.columns:
            if col not in ["area_type", "area_id", "updated_at"]:
                c_val = county[col]
                s_val = state[col]
                print(f"{col:<20} {str(c_val):<15} {str(s_val):<15}")
        frequency_ratio = county["events_per_sqmi"] / max(
            state["events_per_sqmi"], 1e-10
        )
        size_ratio = county["avg_hail_size"] / max(state["avg_hail_size"], 1e-10)
        severity_ratio = county["avg_severe_prob"] / max(
            state["avg_severe_prob"], 1e-10
        )
        damaging_ratio = county["damaging_per_sqmi"] / max(
            state["damaging_per_sqmi"], 1e-10
        )

        weights = {"frequency": 0.25, "size": 0.25, "severity": 0.25, "damaging": 0.25}

        risk_score = (
            weights["frequency"] * frequency_ratio
            + weights["size"] * size_ratio
            + weights["severity"] * severity_ratio
            + weights["damaging"] * damaging_ratio
        )

        # Premium adjustment (1.0 = state average, >1 = increase, <1 = decrease)
        # Scale: 10% adjustment per 0.5 deviation from 1.0
        premium_modifier_pct = (risk_score - 1.0) * 20

        print(f"=== Insurance Premium Analysis for {us_postcode} ===")
        print(f"\nRisk Ratios (vs State Average):")
        print(f"  Frequency:  {frequency_ratio:.4f}x")
        print(f"  Hail Size:  {size_ratio:.4f}x")
        print(f"  Severity:   {severity_ratio:.4f}x")
        print(f"  Damaging:   {damaging_ratio:.4f}x")
        print(f"\nCombined Risk Score: {risk_score:.4f}")
        print(f"  (1.0 = state average)")
        print(f"\n{'='*40}")
        print(f"PREMIUM ADJUSTMENT: {premium_modifier_pct:+.2f}%")
        print(f"{'='*40}")

        if premium_modifier_pct > 0:
            print(f"\nHigher risk than state average - premium increase recommended")
        else:
            print(f"\nLower risk than state average - discount eligible")

    _calculate_insurance_premium = calculate_insurance_premium(
        risk_df=_fetch_information_risk_comparison.output
    )

    chain(_fetch_information_risk_comparison, _calculate_insurance_premium)


insurance_premium_adjustment()
