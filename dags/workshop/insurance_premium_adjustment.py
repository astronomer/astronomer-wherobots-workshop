from airflow.sdk import dag, task, chain, Asset, Param
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from airflow.providers.standard.operators.hitl import HITLOperator
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os
from datetime import timedelta

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_RUNTIME = Runtime.TINY
_REGION = Region.AWS_US_WEST_2
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "workshoptestnotebook")


def on_failure_callback(context):
    print(f"Task {context['task_instance'].task_id} failed")
    print(f"Error: {context['exception']}")


@dag(
    schedule=[Asset("hail_comparison_ready")],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "on_failure_callback": on_failure_callback,
        "execution_timeout": timedelta(minutes=30),
    },
    max_consecutive_failed_dag_runs=10,
    dagrun_timeout=timedelta(hours=4),
    tags=["Human-in-the-loop", "retries and callbacks"],
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

        return {
            "state_id": str(state["area_id"]),
            "county_id": str(county["area_id"]),
            "premium_modifier_pct": float(premium_modifier_pct),
            "frequency_ratio": float(frequency_ratio),
            "size_ratio": float(size_ratio),
            "severity_ratio": float(severity_ratio),
            "damaging_ratio": float(damaging_ratio),
            "risk_score": float(risk_score),
        }

    _calculate_insurance_premium = calculate_insurance_premium(
        risk_df=_fetch_information_risk_comparison.output
    )
    _human_confirm_insurance_premium_adjustment = HITLOperator(
        task_id="human_confirm_insurance_premium_adjustment",
        subject="Insurance Premium Adjustment",
        body="""
{%- set data = ti.xcom_pull(task_ids='calculate_insurance_premium') -%}
## Insurance Premium Adjustment Review

**Location:** {{ data.county_id }}, {{ data.state_id }}

---

### Risk Analysis (vs State Average)

| Metric | Ratio |
|--------|-------|
| Frequency | {{ "%.2f"|format(data.frequency_ratio) }}x |
| Hail Size | {{ "%.2f"|format(data.size_ratio) }}x |
| Severity | {{ "%.2f"|format(data.severity_ratio) }}x |
| Damaging Events | {{ "%.2f"|format(data.damaging_ratio) }}x |

---

### Recommendation

- **Risk Score:** {{ "%.2f"|format(data.risk_score) }} *(1.0 = state average)*
- **Premium Adjustment:** {{ "%+.1f"|format(data.premium_modifier_pct) }}%

Please review and confirm or reject this adjustment.
        """,
        options=["Confirm", "Reject"],
        defaults=["Confirm"],
        params={"reason": Param(type="string", default="...")},
    )

    @task
    def process_human_decision(hitl_result):
        print(f"Decision: {hitl_result['chosen_options']}")
        print(f"Reason: {hitl_result['params_input']['reason']}")

    _process_human_decision = process_human_decision(
        _human_confirm_insurance_premium_adjustment.output
    )

    chain(
        _fetch_information_risk_comparison,
        _calculate_insurance_premium,
        _human_confirm_insurance_premium_adjustment,
        _process_human_decision,
    )


insurance_premium_adjustment()
