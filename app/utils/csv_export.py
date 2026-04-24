"""
CSV export utility for daily Axiom output.
Produces the full ranked table in spreadsheet-friendly format.
"""
import csv
import io
from datetime import date
from typing import Any

DAILY_COLUMNS = [
    "date", "game", "pitcher", "pitcher_id", "team", "opponent",
    "market_type", "sportsbook", "line", "under_odds", "implied_under_prob",
    "base_hits", "base_ks", "projected_hits", "projected_ks",
    "HUSI", "KUSI", "interaction_boost", "volatility_penalty",
    "stat_edge", "grade", "confidence", "notes", "data_quality_flag",
]


def rows_to_csv(rows: list[dict[str, Any]]) -> str:
    """
    Convert a list of output dictionaries to a CSV string.
    Each dict should have keys matching DAILY_COLUMNS (extras are ignored).
    Missing keys default to empty string.
    """
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=DAILY_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in DAILY_COLUMNS})
    return output.getvalue()


def model_outputs_to_export_rows(outputs: list[Any]) -> list[dict[str, Any]]:
    """
    Convert ModelOutputDaily ORM objects into flat dicts ready for CSV export.
    `outputs` is a list of ModelOutputDaily instances, each joined with
    Game and ProbablePitcher data.
    """
    rows = []
    for o in outputs:
        game = o.game
        pitcher = o.pitcher_record
        market = o.market_type

        row = {
            "date": str(o.game_date),
            "game": f"{game.away_team} @ {game.home_team}" if game else "",
            "pitcher": pitcher.pitcher_name if pitcher else "",
            "pitcher_id": o.pitcher_id,
            "team": pitcher.team_id if pitcher else "",
            "opponent": "",
            "market_type": market,
            "sportsbook": o.sportsbook or "",
            "line": o.line,
            "under_odds": o.under_odds,
            "implied_under_prob": round(o.implied_under_prob, 4) if o.implied_under_prob else "",
            "base_hits": o.base_hits,
            "base_ks": o.base_ks,
            "projected_hits": round(o.projected_hits, 2) if o.projected_hits else "",
            "projected_ks": round(o.projected_ks, 2) if o.projected_ks else "",
            "HUSI": round(o.husi, 2) if o.husi else "",
            "KUSI": round(o.kusi, 2) if o.kusi else "",
            "interaction_boost": (
                round((o.husi_interaction or 0) + (o.kusi_interaction or 0), 2)
            ),
            "volatility_penalty": (
                round((o.husi_volatility or 0) + (o.kusi_volatility or 0), 2)
            ),
            "stat_edge": round(o.stat_edge, 2) if o.stat_edge is not None else "",
            "grade": o.grade or "",
            "confidence": o.confidence or "",
            "notes": o.notes or "",
            "data_quality_flag": o.data_quality_flag or "",
        }
        rows.append(row)
    return rows
