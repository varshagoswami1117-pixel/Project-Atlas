from collections import defaultdict
from decimal import Decimal

from backend.scripts.synthetic_dataset import _load_manifest, _metric_rows
from backend.agents.workflow import _word_tokens


def test_synthetic_manifest_has_labelled_clients_reports_and_access_links() -> None:
    manifest = _load_manifest()

    assert len(manifest["clients"]) == 5
    assert len(manifest["reports"]) == 5
    assert all(client["name"].startswith("[SYNTHETIC TEST]") for client in manifest["clients"])
    assert all(client["contact_email"].endswith("@example.invalid") for client in manifest["clients"])
    assert all(report["title"].startswith("[SYNTHETIC]") for report in manifest["reports"])
    assert sum(len(client["reports"]) for client in manifest["clients"]) == 15
    assert all(
        {"report-04", "report-05"}.issubset(set(client["reports"]))
        for client in manifest["clients"]
    )


def test_synthetic_metrics_are_consistent_and_unambiguous_for_calculation() -> None:
    manifest = _load_manifest()
    metrics = list(_metric_rows(manifest))

    assert len(metrics) == 32
    assert all("SYNTHETIC" in metric["source_quote"] for metric in metrics)

    annual_values = [Decimal(value) for _, value in manifest["annual_values"]]
    assert annual_values[0] == Decimal("100")
    assert annual_values[-1] == Decimal("100") * Decimal("1.1") ** 5

    grouped_years: dict[tuple[str, str, str, str], set[int]] = defaultdict(set)
    for metric in metrics:
        key = (
            metric["metric_name"],
            metric["unit"],
            metric["geography"],
            metric["segment"],
        )
        grouped_years[key].add(int(metric["year"]))
    eligible_series = [years for years in grouped_years.values() if len(years) > 1]
    assert len(eligible_series) == 1
    assert eligible_series[0] == {2020, 2021, 2022, 2023, 2024, 2025}

    query_tokens = _word_tokens("Calculate CAGR: Europe market revenue from 2020 to 2025.")
    matched_report_keys = {
        metric["report_key"]
        for metric in metrics
        if query_tokens
        & _word_tokens(
            " ".join(
                str(metric.get(field) or "")
                for field in (
                    "metric_name",
                    "unit",
                    "geography",
                    "segment",
                    "source_quote",
                )
            )
        )
    }
    assert matched_report_keys == {"report-01"}

    control_values = {
        metric["value"]
        for metric in metrics
        if metric["metric_name"] == "SYNTHETIC discrepancy control measurement"
    }
    assert control_values == {"100", "101"}
