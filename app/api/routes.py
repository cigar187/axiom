"""
FastAPI route handlers for all Axiom API endpoints.

Endpoints:
  GET  /health
  GET  /v1/pitchers/today
  GET  /v1/rankings/today
  GET  /v1/pitchers/{id}/profile
  POST /v1/tasks/run-daily           (protected by AXIOM_INTERNAL_TOKEN)
  GET  /v1/exports/daily.csv
"""
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.config import settings
from app.models.base import get_db
from app.models.models import Game, ProbablePitcher, ModelOutputDaily, PitcherFeaturesDaily, MLModelOutput
from app.schemas.schemas import (
    HealthResponse,
    PitchersTodayResponse,
    PitcherTodayRow,
    RankingsTodayResponse,
    RankingRow,
    PitcherProfile,
    HUSIDetail,
    KUSIDetail,
    BlockScores,
    PropLine,
    RunDailyRequest,
    RunDailyResponse,
)
from app.tasks.pipeline import run_daily_pipeline
from app.utils.csv_export import rows_to_csv, model_outputs_to_export_rows
from app.utils.logging import get_logger
from app.utils.teams import get_team_name, get_team_abbrev
from app.core.products import PRODUCT_CATALOG, BUNDLES, product_tags_for_response

log = get_logger("api")

router = APIRouter()


# ─────────────────────────────────────────────────────────────
# /health
# ─────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Quick health check. Returns ok if the server is running."""
    return HealthResponse()


# ─────────────────────────────────────────────────────────────
# /v1/pitchers/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/pitchers/today", response_model=PitchersTodayResponse, tags=["Pitchers"])
async def pitchers_today(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
):
    """
    Full ranked table of all scored pitchers for a given date.
    Returns HUSI, KUSI, grades, prop lines, and projections for every pitcher.
    """
    query_date = target_date or date.today()

    # Load all model outputs for the date, joined with game and pitcher info
    stmt = (
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(ModelOutputDaily.game_date == query_date)
        .order_by(ModelOutputDaily.husi.desc())
    )
    rows = (await db.execute(stmt)).all()

    # Permanent fix: if today has no data (common after midnight UTC / 7 PM Eastern),
    # fall back to the most recent date that has scored pitchers.
    # This prevents the report from going empty just because the UTC clock rolled over.
    if not rows and not target_date:
        fallback_stmt = (
            select(ModelOutputDaily.game_date)
            .order_by(ModelOutputDaily.game_date.desc())
            .limit(1)
        )
        fallback_result = await db.execute(fallback_stmt)
        fallback_date = fallback_result.scalar()
        if fallback_date and fallback_date != query_date:
            query_date = fallback_date
            stmt = (
                select(ModelOutputDaily, ProbablePitcher, Game)
                .join(ProbablePitcher, and_(
                    ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
                    ModelOutputDaily.game_id == ProbablePitcher.game_id,
                ))
                .join(Game, ModelOutputDaily.game_id == Game.game_id)
                .where(ModelOutputDaily.game_date == query_date)
                .order_by(ModelOutputDaily.husi.desc())
            )
            rows = (await db.execute(stmt)).all()

    if not rows:
        return PitchersTodayResponse(
            date=query_date,
            generated_at=datetime.utcnow(),
            pitcher_count=0,
            pitchers=[],
        )

    # Group by pitcher_id — one pitcher has rows for both markets
    pitcher_map: dict[str, dict] = {}
    for output, pitcher, game in rows:
        pid = output.pitcher_id
        if pid not in pitcher_map:
            pitcher_map[pid] = {
                "pitcher": pitcher,
                "game": game,
                "hits": None,
                "ks": None,
            }
        if output.market_type == "hits_allowed":
            pitcher_map[pid]["hits"] = output
        elif output.market_type == "strikeouts":
            pitcher_map[pid]["ks"] = output

    # Pull ML outputs for today in one query
    ml_stmt = select(MLModelOutput).where(MLModelOutput.game_date == query_date)
    ml_rows = (await db.execute(ml_stmt)).scalars().all()
    ml_by_pitcher = {m.pitcher_id: m for m in ml_rows}

    pitcher_rows = []
    for pid, pdata in pitcher_map.items():
        pitcher = pdata["pitcher"]
        game = pdata["game"]
        hits_out = pdata["hits"]
        ks_out = pdata["ks"]

        primary = hits_out or ks_out
        if primary is None:
            continue

        pitcher_team_name = get_team_name(pitcher.team_id) or ""
        opponent = (
            game.away_team if pitcher_team_name == game.home_team else game.home_team
        ) if game else ""

        ml = ml_by_pitcher.get(pid)

        row = PitcherTodayRow(
            date=query_date,
            game=f"{game.away_team} @ {game.home_team}" if game else "",
            pitcher=pitcher.pitcher_name,
            pitcher_id=pitcher.pitcher_id,
            team=get_team_abbrev(pitcher.team_id) or pitcher.team_id,
            team_name=get_team_name(pitcher.team_id),
            opponent=opponent,
            opponent_name=get_team_name(opponent),
            handedness=pitcher.handedness,
            hits_line=hits_out.line if hits_out else None,
            hits_under_odds=hits_out.under_odds if hits_out else None,
            hits_implied_under_prob=hits_out.implied_under_prob if hits_out else None,
            base_hits=hits_out.base_hits if hits_out else None,
            projected_hits=hits_out.projected_hits if hits_out else None,
            hits_edge=hits_out.stat_edge if hits_out else None,
            husi=hits_out.husi if hits_out else None,
            husi_grade=hits_out.grade if hits_out else None,
            k_line=ks_out.line if ks_out else None,
            k_under_odds=ks_out.under_odds if ks_out else None,
            k_implied_under_prob=ks_out.implied_under_prob if ks_out else None,
            base_ks=ks_out.base_ks if ks_out else None,
            projected_ks=ks_out.projected_ks if ks_out else None,
            k_edge=ks_out.stat_edge if ks_out else None,
            kusi=ks_out.kusi if ks_out else None,
            kusi_grade=ks_out.grade if ks_out else None,
            interaction_boost_husi=primary.husi_interaction,
            interaction_boost_kusi=primary.kusi_interaction,
            volatility_penalty_husi=primary.husi_volatility,
            volatility_penalty_kusi=primary.kusi_volatility,
            confidence=primary.confidence,
            notes=primary.notes,
            data_quality_flag=primary.data_quality_flag,
            # ML Engine 2
            ml_husi=ml.ml_husi if ml else None,
            ml_kusi=ml.ml_kusi if ml else None,
            ml_husi_grade=ml.ml_husi_grade if ml else None,
            ml_kusi_grade=ml.ml_kusi_grade if ml else None,
            ml_proj_hits=ml.ml_proj_hits if ml else None,
            ml_proj_ks=ml.ml_proj_ks if ml else None,
            husi_signal=ml.husi_divergence if ml else None,
            kusi_signal=ml.kusi_divergence if ml else None,
            # B2B product tags
            product_tags=product_tags_for_response(),
            # SKU #37, #14, #38 — live-scored fields; None when pulled from DB
            # (these are applied at pipeline scoring time, not stored as separate columns yet)
            catcher_name=None,
            catcher_strike_rate=None,
            catcher_framing_label=None,
            catcher_kusi_adj=None,
            tfi_rest_hours=None,
            tfi_tz_shift=None,
            tfi_getaway_day=None,
            tfi_cross_timezone=None,
            tfi_penalty_pct=None,
            tfi_label=None,
            vaa_degrees=None,
            extension_ft=None,
            vaa_flat=None,
            extension_elite=None,
            # Risk profile — stored per-row in model_outputs_daily
            risk_score=primary.risk_score,
            risk_tier=primary.risk_tier,
            risk_flags=primary.risk_flags,
            pff_score=primary.pff_score,
            pff_label=primary.pff_label,
            # ── Merlin Simulation Engine outputs (N=2000)
            sim_median_hits=primary.sim_median_hits,
            sim_median_ks=primary.sim_median_ks,
            sim_over_pct_hits=primary.sim_over_pct_hits,
            sim_under_pct_hits=primary.sim_under_pct_hits,
            sim_p5_hits=primary.sim_p5_hits,
            sim_p95_hits=primary.sim_p95_hits,
            sim_over_pct_ks=primary.sim_over_pct_ks,
            sim_under_pct_ks=primary.sim_under_pct_ks,
            sim_p5_ks=primary.sim_p5_ks,
            sim_p95_ks=primary.sim_p95_ks,
            sim_confidence_hits=primary.sim_confidence_hits,
            sim_confidence_ks=primary.sim_confidence_ks,
            sim_kill_streak_prob=primary.sim_kill_streak_prob,
        )
        pitcher_rows.append(row)

    return PitchersTodayResponse(
        date=query_date,
        generated_at=datetime.utcnow(),
        pitcher_count=len(pitcher_rows),
        pitchers=pitcher_rows,
    )


# ─────────────────────────────────────────────────────────────
# /v1/rankings/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/rankings/today", response_model=RankingsTodayResponse, tags=["Rankings"])
async def rankings_today(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    market: Optional[str] = Query(default=None, description="Filter: 'strikeouts' or 'hits_allowed'"),
    db: AsyncSession = Depends(get_db),
):
    """
    Ranked list of today's strongest under signals, sorted by stat_edge descending.
    Combines both markets unless filtered.
    """
    query_date = target_date or date.today()

    stmt = (
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(ModelOutputDaily.game_date == query_date)
    )
    if market:
        stmt = stmt.where(ModelOutputDaily.market_type == market)

    stmt = stmt.order_by(ModelOutputDaily.stat_edge.desc().nulls_last())
    rows = (await db.execute(stmt)).all()

    rankings = []
    for rank, (output, pitcher, game) in enumerate(rows, start=1):
        index_score = (
            output.husi if output.market_type == "hits_allowed" else output.kusi
        )
        p_team_abbrev_rank = get_team_abbrev(pitcher.team_id) or pitcher.team_id
        p_team_name_rank   = get_team_name(pitcher.team_id) or ""
        opp_rank = (
            game.away_team if p_team_name_rank == game.home_team else game.home_team
        ) if game else ""
        rankings.append(RankingRow(
            rank=rank,
            pitcher=pitcher.pitcher_name,
            pitcher_id=pitcher.pitcher_id,
            team=p_team_abbrev_rank,
            opponent=opp_rank,
            market_type=output.market_type,
            line=output.line,
            under_odds=output.under_odds,
            projection=output.projected_hits if output.market_type == "hits_allowed" else output.projected_ks,
            edge=output.stat_edge,
            index_score=index_score,
            grade=output.grade,
            confidence=output.confidence,
            data_quality_flag=output.data_quality_flag,
        ))

    return RankingsTodayResponse(
        date=query_date,
        generated_at=datetime.utcnow(),
        rankings=rankings,
    )


# ─────────────────────────────────────────────────────────────
# /v1/pitchers/{id}/profile
# ─────────────────────────────────────────────────────────────

@router.get("/v1/pitchers/{pitcher_id}/profile", response_model=PitcherProfile, tags=["Pitchers"])
async def pitcher_profile(
    pitcher_id: str,
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
):
    """
    Deep dive on one pitcher — all scores, projections, prop lines, and feature blocks.
    """
    query_date = target_date or date.today()

    # Load model outputs for both markets
    stmt = (
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(
            and_(
                ModelOutputDaily.pitcher_id == pitcher_id,
                ModelOutputDaily.game_date == query_date,
            )
        )
    )
    rows = (await db.execute(stmt)).all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for pitcher {pitcher_id} on {query_date}.",
        )

    hits_out = ks_out = None
    pitcher_rec = game_rec = None
    for output, pitcher, game in rows:
        pitcher_rec = pitcher
        game_rec = game
        if output.market_type == "hits_allowed":
            hits_out = output
        elif output.market_type == "strikeouts":
            ks_out = output

    primary = hits_out or ks_out

    # Load feature block scores
    feat_stmt = select(PitcherFeaturesDaily).where(
        and_(
            PitcherFeaturesDaily.pitcher_id == pitcher_id,
            PitcherFeaturesDaily.game_date == query_date,
        )
    )
    feat_row = (await db.execute(feat_stmt)).scalar_one_or_none()

    blocks = BlockScores(
        owc=feat_row.owc_score if feat_row else None,
        pcs=feat_row.pcs_score if feat_row else None,
        ens=feat_row.ens_score if feat_row else None,
        ops=feat_row.ops_score if feat_row else None,
        uhs=feat_row.uhs_score if feat_row else None,
        dsc=feat_row.dsc_score if feat_row else None,
        ocr=feat_row.ocr_score if feat_row else None,
        pmr=feat_row.pmr_score if feat_row else None,
        per=feat_row.per_score if feat_row else None,
        kop=feat_row.kop_score if feat_row else None,
        uks=feat_row.uks_score if feat_row else None,
        tlr=feat_row.tlr_score if feat_row else None,
    )

    pitcher_team_name_p = get_team_name(pitcher_rec.team_id) or ""
    opponent = (
        game_rec.away_team if pitcher_team_name_p == game_rec.home_team else game_rec.home_team
    ) if game_rec else ""

    return PitcherProfile(
        pitcher_id=pitcher_id,
        pitcher_name=pitcher_rec.pitcher_name,
        team=get_team_abbrev(pitcher_rec.team_id) or pitcher_rec.team_id,
        opponent=opponent,
        game_id=primary.game_id,
        game_date=query_date,
        handedness=pitcher_rec.handedness,
        lineup_confirmed=feat_row.lineup_confirmed if feat_row else False,
        umpire_confirmed=feat_row.umpire_confirmed if feat_row else False,
        data_quality_flag=primary.data_quality_flag,
        husi=HUSIDetail(
            husi=primary.husi,
            husi_base=primary.husi_base,
            husi_interaction=primary.husi_interaction,
            husi_volatility=primary.husi_volatility,
            grade=hits_out.grade if hits_out else None,
        ),
        kusi=KUSIDetail(
            kusi=primary.kusi,
            kusi_base=primary.kusi_base,
            kusi_interaction=primary.kusi_interaction,
            kusi_volatility=primary.kusi_volatility,
            grade=ks_out.grade if ks_out else None,
        ),
        block_scores=blocks,
        hits_prop=PropLine(
            sportsbook=hits_out.sportsbook if hits_out else None,
            market_type="hits_allowed",
            line=hits_out.line if hits_out else None,
            over_odds=None,
            under_odds=hits_out.under_odds if hits_out else None,
            implied_under_prob=hits_out.implied_under_prob if hits_out else None,
        ) if hits_out else None,
        k_prop=PropLine(
            sportsbook=ks_out.sportsbook if ks_out else None,
            market_type="strikeouts",
            line=ks_out.line if ks_out else None,
            over_odds=None,
            under_odds=ks_out.under_odds if ks_out else None,
            implied_under_prob=ks_out.implied_under_prob if ks_out else None,
        ) if ks_out else None,
        base_hits=hits_out.base_hits if hits_out else None,
        projected_hits=hits_out.projected_hits if hits_out else None,
        base_ks=ks_out.base_ks if ks_out else None,
        projected_ks=ks_out.projected_ks if ks_out else None,
        hits_edge=hits_out.stat_edge if hits_out else None,
        k_edge=ks_out.stat_edge if ks_out else None,
        notes=primary.notes,
    )


# ─────────────────────────────────────────────────────────────
# /v1/products  — public B2B product catalog
# ─────────────────────────────────────────────────────────────

@router.get("/v1/products", tags=["Products"])
async def product_catalog():
    """
    Full Axiom B2B product catalog.

    Returns every individually licensable data product (SKUs #27-#36)
    and the available bundle packages (SKUs #50-#52).
    Each entry includes the SKU number, name, description, and the
    specific API response fields delivered by that product.
    """
    return {
        "catalog": list(PRODUCT_CATALOG.values()),
        "bundles": list(BUNDLES.values()),
        "total_products": len(PRODUCT_CATALOG),
        "total_bundles": len(BUNDLES),
        "contact": "contact@gtmvelo.com",
        "note": (
            "All data products update daily before first pitch. "
            "SKU #34 (MGS) also delivers live in-game updates."
        ),
    }


# ─────────────────────────────────────────────────────────────
# /v1/tasks/run-daily  (protected)
# ─────────────────────────────────────────────────────────────

@router.post("/v1/tasks/run-daily", tags=["Tasks"])
async def run_daily_task(
    body: RunDailyRequest,
    background_tasks: BackgroundTasks,
    axiom_internal_token: Optional[str] = Header(default=None, alias="AXIOM-INTERNAL-TOKEN"),
):
    """
    Trigger the daily scoring pipeline.
    Returns immediately — pipeline runs in the background (takes 3-8 min).
    Results are saved to the database; read them via GET /v1/pitchers/today.
    Protected: requires AXIOM-INTERNAL-TOKEN header.
    """
    if axiom_internal_token != settings.AXIOM_INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid or missing AXIOM-INTERNAL-TOKEN.")

    target_date = body.target_date or date.today()
    dry_run = body.dry_run
    log.info("Pipeline triggered (background)", date=str(target_date), dry_run=dry_run)

    async def _run_pipeline() -> None:
        from app.models.base import AsyncSessionLocal
        try:
            async with AsyncSessionLocal() as session:
                result = await run_daily_pipeline(session, target_date=target_date, dry_run=dry_run)
                log.info("Background pipeline complete",
                         status=result.get("status"),
                         pitchers=result.get("pitchers_scored"),
                         elapsed=result.get("elapsed_seconds"))
        except Exception as exc:
            log.error("Background pipeline failed", error=str(exc))

    background_tasks.add_task(_run_pipeline)

    return {
        "status": "started",
        "target_date": str(target_date),
        "dry_run": dry_run,
        "message": (
            "Pipeline is running in the background. "
            f"Scoring pitchers for {target_date}. "
            "Results will be available at GET /v1/pitchers/today in 3-8 minutes."
        ),
    }


# ─────────────────────────────────────────────────────────────
# /v1/reports/merlin-board
# ─────────────────────────────────────────────────────────────

@router.get("/v1/reports/merlin-board", tags=["Reports"])
async def merlin_board(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
):
    """
    The Merlin Board — combined hits + Ks report with simulation spreads.

    Returns every starter for the day sorted by HUSI (best hit suppression first),
    with formula scores, grades, and simulation floor/median/ceiling for both props.

    Color tier logic (for display clients):
      Hits:  GREEN = HUSI >= 57 (A/A+)  |  YELLOW = HUSI 45-57 (B/C)  |  RED = HUSI < 45 (D)
      Ks:    GREEN = K-CEIL >= 9.0      |  YELLOW = K-CEIL 7.0-9.0    |  RED = K-CEIL < 7.0
    """
    td = target_date or date.today()
    result = await db.execute(
        select(ModelOutputDaily)
        .where(
            ModelOutputDaily.game_date == td,
            ModelOutputDaily.market_type == "hits",
        )
        .order_by(ModelOutputDaily.husi.desc().nullslast())
    )
    hits_rows = result.scalars().all()

    # Pull matching K rows for same pitchers
    k_result = await db.execute(
        select(ModelOutputDaily)
        .where(
            ModelOutputDaily.game_date == td,
            ModelOutputDaily.market_type == "strikeouts",
        )
    )
    k_rows_by_pitcher = {r.pitcher_id: r for r in k_result.scalars().all()}

    from app.utils.teams import get_team_abbrev, get_team_name

    board = []
    for h in hits_rows:
        k = k_rows_by_pitcher.get(h.pitcher_id)

        # Color tier for hits side
        husi_val = h.husi or 0.0
        if husi_val >= 57:
            hits_color = "GREEN"
        elif husi_val >= 45:
            hits_color = "YELLOW"
        else:
            hits_color = "RED"

        # Color tier for K side
        k_ceil = (k.sim_p95_ks if k else None) or 0.0
        if k_ceil >= 9.0:
            ks_color = "GREEN"
        elif k_ceil >= 7.0:
            ks_color = "YELLOW"
        else:
            ks_color = "RED"

        opp_name = get_team_name(h.away_team_id or "") if h.home_away == "home" else get_team_name(h.home_team_id or "")

        board.append({
            "pitcher":       h.pitcher_name,
            "opponent":      opp_name,
            "game":          h.game_id,
            # ── Hits
            "husi":          h.husi,
            "husi_grade":    h.husi_grade,
            "hits_line":     h.sportsbook_line,
            "h_floor":       h.sim_p5_hits,
            "h_median":      h.sim_median_hits,
            "h_ceil":        h.sim_p95_hits,
            "h_over_pct":    h.sim_over_pct_hits,
            "h_under_pct":   h.sim_under_pct_hits,
            "hits_color":    hits_color,
            # ── Ks
            "kusi":          k.kusi if k else None,
            "kusi_grade":    k.kusi_grade if k else None,
            "k_line":        k.sportsbook_line if k else None,
            "k_floor":       k.sim_p5_ks if k else None,
            "k_median":      k.sim_median_ks if k else None,
            "k_ceil":        k.sim_p95_ks if k else None,
            "k_over_pct":    k.sim_over_pct_ks if k else None,
            "k_under_pct":   k.sim_under_pct_ks if k else None,
            "ks_color":      ks_color,
            "kill_streak_prob": k.sim_kill_streak_prob if k else None,
        })

    return {
        "date":    str(td),
        "count":   len(board),
        "board":   board,
        "legend": {
            "hits_color":  "GREEN=strong Under (HUSI>=57) | YELLOW=neutral | RED=risky",
            "ks_color":    "GREEN=K Over potential (ceil>=9) | YELLOW=moderate | RED=low",
        },
    }


# ─────────────────────────────────────────────────────────────
# /v1/exports/daily.csv
# ─────────────────────────────────────────────────────────────

@router.get("/v1/exports/daily.csv", tags=["Exports"])
async def export_daily_csv(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
):
    """
    Download today's full output as a spreadsheet-ready CSV file.
    Includes all columns: HUSI, KUSI, grades, props, projections, edges.
    """
    query_date = target_date or date.today()

    stmt = (
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(ModelOutputDaily.game_date == query_date)
        .order_by(ModelOutputDaily.stat_edge.desc().nulls_last())
    )
    rows = (await db.execute(stmt)).all()

    if not rows:
        return Response(
            content="date,game,pitcher\nNo data for this date.\n",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=axiom_{query_date}.csv"},
        )

    export_rows = []
    for output, pitcher, game in rows:
        p_team_name_csv = get_team_name(pitcher.team_id) or ""
        opponent = (
            game.away_team if p_team_name_csv == game.home_team else game.home_team
        ) if game else ""

        export_rows.append({
            "date": str(query_date),
            "game": f"{game.away_team} @ {game.home_team}" if game else "",
            "pitcher": pitcher.pitcher_name,
            "pitcher_id": pitcher.pitcher_id,
            "team": get_team_abbrev(pitcher.team_id) or pitcher.team_id,
            "opponent": opponent,
            "market_type": output.market_type,
            "sportsbook": output.sportsbook or "",
            "line": output.line,
            "under_odds": output.under_odds,
            "implied_under_prob": round(output.implied_under_prob, 4) if output.implied_under_prob else "",
            "base_hits": output.base_hits,
            "base_ks": output.base_ks,
            "projected_hits": round(output.projected_hits, 2) if output.projected_hits else "",
            "projected_ks": round(output.projected_ks, 2) if output.projected_ks else "",
            "HUSI": round(output.husi, 2) if output.husi else "",
            "KUSI": round(output.kusi, 2) if output.kusi else "",
            "interaction_boost": round((output.husi_interaction or 0) + (output.kusi_interaction or 0), 2),
            "volatility_penalty": round((output.husi_volatility or 0) + (output.kusi_volatility or 0), 2),
            "stat_edge": round(output.stat_edge, 2) if output.stat_edge is not None else "",
            "grade": output.grade or "",
            "confidence": output.confidence or "",
            "notes": output.notes or "",
            "data_quality_flag": output.data_quality_flag or "",
        })

    csv_content = rows_to_csv(export_rows)
    filename = f"axiom_{query_date}.csv"

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/tasks/run-backfill
# One-time endpoint to retroactively load the full 2026 season
# into ml_training_samples so the ML engine has data immediately.
# Protected by the same internal token as run-daily.
# ─────────────────────────────────────────────────────────────

@router.post("/v1/tasks/run-backfill", tags=["Tasks"])
async def run_backfill_endpoint(
    background_tasks: BackgroundTasks,
    axiom_internal_token: Optional[str] = Header(default=None, alias="AXIOM-INTERNAL-TOKEN"),
):
    """
    Retroactively load the full 2026 season into ml_training_samples.
    Returns immediately — actual work runs in the background (takes 5-15 min).
    Safe to call multiple times — uses ON CONFLICT DO NOTHING so no duplicates.
    Check Cloud Run logs to monitor progress.
    """
    if axiom_internal_token != settings.AXIOM_INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def _run_backfill_task() -> None:
        from app.ml.backfill import run_backfill
        from app.models.base import AsyncSessionLocal
        try:
            async with AsyncSessionLocal() as session:
                result = await run_backfill(session, season="2026")
                log.info("Backfill background task complete",
                         loaded=result["loaded"],
                         skipped=result["skipped"],
                         errors=result["errors"])
        except Exception as exc:
            log.error("Backfill background task failed", error=str(exc))

    background_tasks.add_task(_run_backfill_task)

    return {
        "status": "started",
        "message": (
            "Backfill is running in the background. "
            "It will load all 2026 season games into ml_training_samples. "
            "Check Cloud Run logs for progress — look for 'ML backfill complete'."
        ),
    }


# ─────────────────────────────────────────────────────────────
# /v1/risk/today — Daily pitcher risk report (auto-generated)
# ─────────────────────────────────────────────────────────────
@router.get("/v1/risk/today", tags=["Risk"])
async def risk_today(
    date_str: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD (default: today)"),
    tier: Optional[str] = Query(None, description="Filter by risk tier: HIGH, MODERATE, LOW"),
    db: AsyncSession = Depends(get_db),
):
    """
    Daily pitcher risk report — automatically computed each morning by the pipeline.

    No manual commands needed. The Cloud Scheduler job fires at 10 AM ET,
    runs the full pipeline, and stores risk profiles for every probable starter.
    This endpoint reads those stored results.

    Risk Flags:
      ERA_DISASTER    Season ERA ≥ 6.00
      ERA_STRUGGLING  Season ERA 5.00–5.99
      BOOM_BUST       IP variance pattern — early-exit history (the Walker Buehler rule)
      EXTREME_PARK    Pitching at Coors, Chase, GABP, Citizens Bank, Globe Life, etc.
      HITTER_PARK     Any park with above-average hit environment
      HIGH_H9         Season H/9 ≥ 9.5
      TFI_ACTIVE      Travel & Fatigue penalty triggered today
      COLD_START      Pitcher PFF grade is COLD or STRUGGLING
      COMBO_RISK      3 or more flags active simultaneously — strong OVER lean

    Risk Tiers:
      HIGH     Score ≥ 20 — multiple danger factors stacking
      MODERATE Score 8-19 — at least one significant risk factor
      LOW      Score < 8  — clean profile
    """
    query_date = date.today()
    if date_str:
        try:
            query_date = date.fromisoformat(date_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # Pull one row per pitcher (hits_allowed market has the full risk data)
    stmt = (
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(
            and_(
                ModelOutputDaily.game_date == query_date,
                ModelOutputDaily.market_type == "hits_allowed",
            )
        )
        .order_by(ModelOutputDaily.risk_score.desc())
    )

    rows = (await db.execute(stmt)).all()

    if not rows:
        return {
            "date": query_date.isoformat(),
            "message": "No risk data found for this date. Pipeline may not have run yet.",
            "pitchers": [],
            "summary": {},
        }

    pitchers = []
    for output, pitcher, game in rows:
        flags = (output.risk_flags or "").split("|") if output.risk_flags else []
        r_tier = output.risk_tier or "LOW"

        if tier and r_tier.upper() != tier.upper():
            continue

        p_team_name_risk = get_team_name(pitcher.team_id) or ""
        opponent = (
            game.away_team if p_team_name_risk == game.home_team else game.home_team
        ) if game else ""

        pitchers.append({
            "pitcher":            pitcher.pitcher_name,
            "team":               get_team_abbrev(pitcher.team_id) or pitcher.team_id,
            "game_id":            output.game_id,
            "risk_score":         output.risk_score or 0,
            "risk_tier":          r_tier,
            "risk_flags":         flags,
            "combo_risk":         output.combo_risk or False,
            "season_era_tier":    output.season_era_tier or "NORMAL",
            "park_extreme":       output.park_extreme or False,
            "park_hits_multiplier": output.park_hits_multiplier or 1.0,
            "husi":               output.husi,
            "projected_hits":     output.projected_hits,
            "grade":              output.grade,
        })

    # Summary counts
    high     = sum(1 for p in pitchers if p["risk_tier"] == "HIGH")
    moderate = sum(1 for p in pitchers if p["risk_tier"] == "MODERATE")
    low      = sum(1 for p in pitchers if p["risk_tier"] == "LOW")
    combos   = sum(1 for p in pitchers if p["combo_risk"])

    return {
        "date": query_date.isoformat(),
        "generated_by": "Axiom daily pipeline (Cloud Scheduler — 10 AM ET)",
        "total_pitchers": len(pitchers),
        "summary": {
            "high_risk":    high,
            "moderate_risk": moderate,
            "low_risk":     low,
            "combo_risk_count": combos,
        },
        "pitchers": pitchers,
    }
