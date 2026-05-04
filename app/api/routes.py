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
import zoneinfo
from typing import Optional

_EASTERN = zoneinfo.ZoneInfo("America/New_York")


def _today_eastern() -> date:
    return datetime.now(tz=_EASTERN).date()

from fastapi import APIRouter, BackgroundTasks, Body, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field
# BackgroundTasks retained for run-daily and run-backfill endpoints
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, and_

from app.config import settings
from app.models.base import get_db
from app.models.models import (
    Game, ProbablePitcher, ModelOutputDaily, PitcherFeaturesDaily, MLModelOutput, ApiKey,
    NFLModelOutputDaily, NFLQBFeaturesDaily,
    NHLGame, NHLModelOutputDaily, NHLGoalieFeaturesDaily, NHLSkaterFeaturesDaily,
)
from app.schemas.schemas import (
    HealthResponse,
    PitchersTodayResponse,
    PitcherTodayRow,
    RankingsTodayResponse,
    RankingRow,
    PitcherProfile,
    HUSIDetail,
    KUSIDetail,
    # TODO: add HSSIDetail and KSSIDetail to schemas.py in next block
    BlockScores,
    PropLine,
    RunDailyRequest,
    PitcherWarningFlag,
    PitcherWarningsResponse,
    BookShieldPitcher,
    BookShieldResponse,
)
from app.tasks.pipeline import run_daily_pipeline
from app.utils.csv_export import rows_to_csv
from app.utils.logging import get_logger
from app.utils.teams import get_team_name, get_team_abbrev
from app.core.products import PRODUCT_CATALOG, BUNDLES, product_tags_for_response

log = get_logger("api")

_CONTACT_EMAIL = "contact@gtmvelo.com"
router = APIRouter()


# ─────────────────────────────────────────────────────────────
# B2B API key authentication dependency
# ─────────────────────────────────────────────────────────────

async def require_api_key(
    x_axiom_key: Optional[str] = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> None:
    if not x_axiom_key:
        raise HTTPException(status_code=401, detail="Missing X-Axiom-Key header.")
    result = await db.execute(
        select(ApiKey).where(ApiKey.key == x_axiom_key, ApiKey.active == True)
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(status_code=403, detail="Invalid or inactive API key.")


# ─────────────────────────────────────────────────────────────
# /health
# ─────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health(db: AsyncSession = Depends(get_db)):
    """Health check — returns db status, last pipeline run, and today's pitcher count."""
    from datetime import date, timezone
    from sqlalchemy import func, text
    from app.models.models import ModelOutputDaily

    # 1) Database reachability
    try:
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        db_status = "unreachable"

    # 2) Most recent pipeline run timestamp
    last_run = None
    try:
        result = await db.execute(
            select(func.max(ModelOutputDaily.created_at))
        )
        val = result.scalar_one_or_none()
        if val:
            last_run = val.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    # 3) Pitchers scored today
    scored_today = None
    try:
        result = await db.execute(
            select(func.count(ModelOutputDaily.pitcher_id.distinct()))
            .where(ModelOutputDaily.game_date == date.today())
        )
        scored_today = result.scalar_one_or_none()
    except Exception:
        pass

    return HealthResponse(
        db=db_status,
        last_pipeline_run=last_run,
        pitchers_scored_today=scored_today,
    )


# ─────────────────────────────────────────────────────────────
# /v1/pitchers/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/pitchers/today", response_model=PitchersTodayResponse, tags=["Pitchers"])
async def pitchers_today(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
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
        .order_by(ModelOutputDaily.hssi.desc())
    )
    rows = (await db.execute(stmt)).all()

    # If today has no data, fall back to the most recent date that has scored pitchers.
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
                .order_by(ModelOutputDaily.hssi.desc())
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

    # Pull ops_hook from pitcher_features_daily in one query
    feat_stmt = select(PitcherFeaturesDaily).where(PitcherFeaturesDaily.game_date == query_date)
    feat_rows = (await db.execute(feat_stmt)).scalars().all()
    feat_by_pitcher = {f.pitcher_id: f for f in feat_rows}

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
            hssi=hits_out.hssi if hits_out else None,
            husi=hits_out.husi if hits_out else None,
            hssi_grade=hits_out.grade if hits_out else None,
            husi_grade=hits_out.grade if hits_out else None,
            k_line=ks_out.line if ks_out else None,
            k_under_odds=ks_out.under_odds if ks_out else None,
            k_implied_under_prob=ks_out.implied_under_prob if ks_out else None,
            base_ks=ks_out.base_ks if ks_out else None,
            projected_ks=ks_out.projected_ks if ks_out else None,
            k_edge=ks_out.stat_edge if ks_out else None,
            kssi=ks_out.kssi if ks_out else None,
            kusi=ks_out.kusi if ks_out else None,
            kssi_grade=ks_out.grade if ks_out else None,
            kusi_grade=ks_out.grade if ks_out else None,
            interaction_boost_hssi=primary.hssi_interaction,
            interaction_boost_husi=primary.husi_interaction,
            interaction_boost_kssi=primary.kssi_interaction,
            interaction_boost_kusi=primary.kusi_interaction,
            volatility_penalty_hssi=primary.hssi_volatility,
            volatility_penalty_husi=primary.husi_volatility,
            volatility_penalty_kssi=primary.kssi_volatility,
            volatility_penalty_kusi=primary.kusi_volatility,
            confidence=primary.confidence,
            notes=primary.notes,
            data_quality_flag=primary.data_quality_flag,
            # ML Engine 2
            ml_hssi=ml.ml_hssi if ml else None,
            ml_husi=ml.ml_husi if ml else None,
            ml_kssi=ml.ml_kssi if ml else None,
            ml_kusi=ml.ml_kusi if ml else None,
            ml_hssi_grade=ml.ml_hssi_grade if ml else None,
            ml_husi_grade=ml.ml_husi_grade if ml else None,
            ml_kssi_grade=ml.ml_kssi_grade if ml else None,
            ml_kusi_grade=ml.ml_kusi_grade if ml else None,
            ml_proj_hits=ml.ml_proj_hits if ml else None,
            ml_proj_ks=ml.ml_proj_ks if ml else None,
            hssi_signal=ml.hssi_divergence if ml else None,
            husi_signal=ml.husi_divergence if ml else None,
            kssi_signal=ml.kssi_divergence if ml else None,
            kusi_signal=ml.kusi_divergence if ml else None,
            # Entropy Filter
            hits_entropy=primary.hits_entropy,
            ks_entropy=primary.ks_entropy,
            entropy_label=primary.entropy_label,
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
            ops_hook=feat_by_pitcher[pid].ops_hook if pid in feat_by_pitcher else None,
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
    _: None = Depends(require_api_key),
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
            output.hssi if output.market_type == "hits_allowed" else output.kssi
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

@router.get("/v1/pitchers/warnings", tags=["Pitchers"])
async def pitchers_warnings(
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Returns today's pitcher warning flags with current HSSI/KSSI scores.
    A warning is raised when a pitcher missed their sim median in 2 of their
    last 3 completed starts.
    """
    from sqlalchemy import text as _text

    query_date = target_date or _today_eastern()

    # ── 1. Fetch today's flags
    flag_rows = await db.execute(
        _text("""
            SELECT pitcher_id, pitcher_name, flag_type,
                   actual_ks, floor_ks, actual_hits, floor_hits,
                   ks_misses, hits_misses
            FROM pitcher_warning_flags
            WHERE game_date = :today
            ORDER BY flag_type, pitcher_name
        """),
        {"today": query_date},
    )
    flags = flag_rows.fetchall()

    if not flags:
        return PitcherWarningsResponse(
            date=str(query_date),
            generated_at=datetime.utcnow().isoformat(),
            flag_count=0,
            warnings=[],
        )

    # ── 2. Pull current HSSI/KSSI scores from model_outputs_daily
    pitcher_ids = [str(f.pitcher_id) for f in flags]

    score_rows = await db.execute(
        _text("""
            SELECT
                mod.pitcher_id,
                MAX(CASE WHEN mod.market_type = 'hits_allowed' THEN mod.hssi END) AS hssi,
                MAX(CASE WHEN mod.market_type = 'strikeouts'   THEN mod.kssi END) AS kssi,
                pp.team_id,
                g.home_team,
                g.away_team
            FROM model_outputs_daily mod
            JOIN probable_pitchers pp
              ON pp.pitcher_id = mod.pitcher_id
             AND pp.game_id    = mod.game_id
            JOIN games g
              ON g.game_id = mod.game_id
            WHERE mod.game_date  = :today
              AND mod.pitcher_id = ANY(:pids)
            GROUP BY mod.pitcher_id, pp.team_id, g.home_team, g.away_team
        """),
        {"today": query_date, "pids": pitcher_ids},
    )
    scores = {str(r.pitcher_id): r for r in score_rows.fetchall()}

    # ── 3. Build response
    warnings = []
    for f in flags:
        pid = str(f.pitcher_id)
        sc = scores.get(pid)

        team_abbrev = get_team_abbrev(sc.team_id) if sc else None
        opponent = None
        if sc and team_abbrev:
            pitcher_team_name = get_team_name(sc.team_id) or ""
            opponent = (
                sc.away_team if pitcher_team_name == sc.home_team else sc.home_team
            )

        warnings.append(PitcherWarningFlag(
            pitcher_name=f.pitcher_name,
            team=team_abbrev,
            opponent=opponent,
            flag_type=f.flag_type,
            actual_ks=f.actual_ks,
            floor_ks=f.floor_ks,
            actual_hits=f.actual_hits,
            floor_hits=f.floor_hits,
            ks_misses=f.ks_misses,
            hits_misses=f.hits_misses,
            hssi_score=sc.hssi if sc else None,
            kssi_score=sc.kssi if sc else None,
        ))

    return PitcherWarningsResponse(
        date=str(query_date),
        generated_at=datetime.utcnow().isoformat(),
        flag_count=len(warnings),
        warnings=warnings,
    )


@router.get("/v1/pitchers/bookshield", tags=["Pitchers"])
async def pitchers_bookshield(
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Returns today's pitchers who have NO sportsbook line posted (line IS NULL
    in model_outputs_daily) and have NOT triggered a warning flag today.
    These are 'clean' pitchers the books haven't priced — potential edges.
    """
    from sqlalchemy import text as _text

    query_date = target_date or _today_eastern()

    # ── 1. Find pitchers with a missing line for at least one market today
    no_line_rows = await db.execute(
        _text("""
            SELECT
                mod.pitcher_id,
                pp.pitcher_name,
                pp.team_id,
                g.home_team,
                g.away_team,
                BOOL_OR(mod.line IS NULL AND mod.market_type = 'strikeouts')   AS no_ks_line,
                BOOL_OR(mod.line IS NULL AND mod.market_type = 'hits_allowed') AS no_hits_line,
                MAX(mod.hssi) AS hssi,
                MAX(mod.kssi) AS kssi
            FROM model_outputs_daily mod
            JOIN probable_pitchers pp
              ON pp.pitcher_id = mod.pitcher_id
             AND pp.game_id    = mod.game_id
            JOIN games g
              ON g.game_id = mod.game_id
            WHERE mod.game_date = :today
            GROUP BY mod.pitcher_id, pp.pitcher_name, pp.team_id, g.home_team, g.away_team
        """),
        {"today": query_date},
    )
    candidates = [r for r in no_line_rows.fetchall() if r.no_ks_line or r.no_hits_line]

    if not candidates:
        return BookShieldResponse(
            date=str(query_date),
            generated_at=datetime.utcnow().isoformat(),
            pitcher_count=0,
            pitchers=[],
        )

    # ── 2. Exclude any pitcher who has a warning flag today
    flagged_ids = await db.execute(
        _text("""
            SELECT pitcher_id FROM pitcher_warning_flags
            WHERE game_date = :today
        """),
        {"today": query_date},
    )
    flagged = {str(r.pitcher_id) for r in flagged_ids.fetchall()}

    clean = [r for r in candidates if str(r.pitcher_id) not in flagged]

    # ── 3. Build response
    pitchers = []
    for r in clean:
        team_abbrev = get_team_abbrev(r.team_id) if r.team_id else None
        pitcher_team_name = get_team_name(r.team_id) or ""
        opponent = (
            r.away_team if pitcher_team_name == r.home_team else r.home_team
        ) if team_abbrev else None

        if r.no_ks_line and r.no_hits_line:
            market = "both"
        elif r.no_ks_line:
            market = "strikeouts"
        else:
            market = "hits_allowed"

        pitchers.append(BookShieldPitcher(
            pitcher_name=r.pitcher_name,
            team=team_abbrev,
            opponent=opponent,
            no_line_market=market,
            hssi_score=r.hssi,
            kssi_score=r.kssi,
        ))

    return BookShieldResponse(
        date=str(query_date),
        generated_at=datetime.utcnow().isoformat(),
        pitcher_count=len(pitchers),
        pitchers=sorted(pitchers, key=lambda p: p.pitcher_name),
    )


@router.get("/v1/pitchers/{pitcher_id}/profile", response_model=PitcherProfile, tags=["Pitchers"])
async def pitcher_profile(
    pitcher_id: str,
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
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
        # TODO: add HSSIDetail and KSSIDetail to schemas.py in next block, then wire here:
        # hssi=HSSIDetail(hssi=primary.hssi, hssi_base=primary.hssi_base,
        #                  hssi_interaction=primary.hssi_interaction,
        #                  hssi_volatility=primary.hssi_volatility,
        #                  grade=hits_out.grade if hits_out else None),
        kusi=KUSIDetail(
            kusi=primary.kusi,
            kusi_base=primary.kusi_base,
            kusi_interaction=primary.kusi_interaction,
            kusi_volatility=primary.kusi_volatility,
            grade=ks_out.grade if ks_out else None,
        ),
        # kssi=KSSIDetail(kssi=primary.kssi, kssi_base=primary.kssi_base,
        #                  kssi_interaction=primary.kssi_interaction,
        #                  kssi_volatility=primary.kssi_volatility,
        #                  grade=ks_out.grade if ks_out else None),
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
async def product_catalog(_: None = Depends(require_api_key)):
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
        "contact": _CONTACT_EMAIL,
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
    _: None = Depends(require_api_key),
):
    """
    The Merlin Board — combined hits + Ks report with simulation spreads.

    Returns every starter for the day sorted by HSSI (best hit suppression first),
    with formula scores, grades, and simulation floor/median/ceiling for both props.

    Color tier logic (for display clients):
      Hits:  GREEN = HSSI >= 57 (A/A+)  |  YELLOW = HSSI 45-57 (B/C)  |  RED = HSSI < 45 (D)
      Ks:    GREEN = K-CEIL >= 9.0      |  YELLOW = K-CEIL 7.0-9.0    |  RED = K-CEIL < 7.0
    """
    td = target_date or date.today()

    try:
        # Join with ProbablePitcher and Game to get pitcher names and team info
        hits_result = await db.execute(
        select(ModelOutputDaily, ProbablePitcher, Game)
        .join(ProbablePitcher, and_(
            ModelOutputDaily.pitcher_id == ProbablePitcher.pitcher_id,
            ModelOutputDaily.game_id == ProbablePitcher.game_id,
        ))
        .join(Game, ModelOutputDaily.game_id == Game.game_id)
        .where(
            ModelOutputDaily.game_date == td,
            ModelOutputDaily.market_type == "hits_allowed",
        )
        .order_by(ModelOutputDaily.hssi.desc().nulls_last())
    )
    hits_rows = hits_result.all()

    # Pull matching K rows for same pitchers
    k_result = await db.execute(
        select(ModelOutputDaily)
        .where(
            ModelOutputDaily.game_date == td,
            ModelOutputDaily.market_type == "strikeouts",
        )
    )
    k_rows_by_pitcher = {r.pitcher_id: r for r in k_result.scalars().all()}

    board = []
    for h_out, h_pitcher, h_game in hits_rows:
        k = k_rows_by_pitcher.get(h_out.pitcher_id)

        # Determine opponent from game info
        pitcher_team_name_mb = get_team_name(h_pitcher.team_id) or ""
        opponent_mb = (
            h_game.away_team if pitcher_team_name_mb == h_game.home_team else h_game.home_team
        ) if h_game else ""

        # Color tier for hits side
        hssi_val = h_out.hssi or 0.0
        if hssi_val >= 57:
            hits_color = "GREEN"
        elif hssi_val >= 45:
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

        board.append({
            "pitcher":       h_pitcher.pitcher_name,
            "team":          get_team_abbrev(h_pitcher.team_id) or h_pitcher.team_id,
            "opponent":      opponent_mb,
            "game":          h_out.game_id,
            # ── Hits
            "hssi":          h_out.hssi,
            "hssi_grade":    h_out.grade,
            "husi":          h_out.husi,
            "husi_grade":    h_out.grade,
            "hits_line":     h_out.line,
            "h_floor":       h_out.sim_p5_hits,
            "h_median":      h_out.sim_median_hits,
            "h_ceil":        h_out.sim_p95_hits,
            "h_over_pct":    h_out.sim_over_pct_hits,
            "h_under_pct":   h_out.sim_under_pct_hits,
            "hits_color":    hits_color,
            # ── Ks
            "kssi":          k.kssi if k else None,
            "kssi_grade":    k.grade if k else None,
            "kusi":          k.kusi if k else None,
            "kusi_grade":    k.grade if k else None,
            "k_line":        k.line if k else None,
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
    except Exception as exc:
        log.error("merlin_board: query failed", error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to load Merlin board.")


# ─────────────────────────────────────────────────────────────
# /v1/exports/daily.csv
# ─────────────────────────────────────────────────────────────

@router.get("/v1/exports/daily.csv", tags=["Exports"])
async def export_daily_csv(
    target_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to today."),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
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
            "HSSI": round(output.hssi, 2) if output.hssi else "",
            "HUSI": round(output.husi, 2) if output.husi else "",
            "KSSI": round(output.kssi, 2) if output.kssi else "",
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

class BackfillRequest(BaseModel):
    season: str = Field(default_factory=lambda: str(datetime.now().year))


@router.post("/v1/tasks/run-backfill", tags=["Tasks"])
async def run_backfill_endpoint(
    background_tasks: BackgroundTasks,
    body: BackfillRequest = BackfillRequest(),
    axiom_internal_token: Optional[str] = Header(default=None, alias="AXIOM-INTERNAL-TOKEN"),
):
    """
    Retroactively load a full MLB season into ml_training_samples.
    Accepts optional JSON body: {"season": "2025"} — defaults to "2026".
    Returns immediately — actual work runs in the background (takes 20-35 min for a full season).
    Safe to call multiple times — uses ON CONFLICT DO NOTHING so no duplicates.
    Check Cloud Run logs to monitor progress.
    """
    if axiom_internal_token != settings.AXIOM_INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    season = body.season

    async def _run_backfill_task() -> None:
        from app.ml.backfill import run_backfill
        from app.models.base import AsyncSessionLocal
        try:
            async with AsyncSessionLocal() as session:
                result = await run_backfill(session, season=season)
                log.info("Backfill background task complete",
                         season=season,
                         loaded=result["loaded"],
                         skipped=result["skipped"],
                         errors=result["errors"])
        except Exception as exc:
            log.error("Backfill background task failed", season=season, error=str(exc))

    background_tasks.add_task(_run_backfill_task)

    return {
        "status": "started",
        "season": season,
        "message": (
            f"Backfill is running in the background for the {season} season. "
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
    _: None = Depends(require_api_key),
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

    try:
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
            "hssi":               output.hssi,
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
    except Exception as exc:
        log.error("risk_today: query failed", error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to load risk report.")


# ── NFL QB Endpoints ──────────────────────────────────────────


class NFLRunWeeklyRequest(BaseModel):
    dry_run: bool = False


# ─────────────────────────────────────────────────────────────
# Shared NFL helpers
# ─────────────────────────────────────────────────────────────

def _nfl_season_year_routes(today: date) -> int:
    return today.year if today.month >= 8 else today.year - 1


async def _get_current_nfl_week(db: AsyncSession, season_year: int) -> int:
    """Return the most recent week that has scored data in nfl_model_outputs_daily."""
    result = await db.execute(
        select(func.max(NFLModelOutputDaily.week))
        .where(NFLModelOutputDaily.season_year == season_year)
    )
    return result.scalar_one_or_none() or 1


def _merge_nfl_rows(rows) -> list[dict]:
    """
    Group nfl_model_outputs_daily rows (one per market per QB) into one
    combined dict per QB. Returns list ordered by insertion order.
    """
    qb_map: dict[str, dict] = {}
    for output in rows:
        key = f"{output.game_id}::{output.qb_name}"
        if key not in qb_map:
            qb_map[key] = {
                "qb_name":   output.qb_name,
                "team":      output.team,
                "opponent":  output.opponent,
                "week":      output.week,
                "game_id":   output.game_id,
                "yards":     None,
                "tds":       None,
            }
        if output.market == "passing_yards":
            qb_map[key]["yards"] = output
        elif output.market == "touchdowns":
            qb_map[key]["tds"] = output
    return list(qb_map.values())


def _build_qb_card(entry: dict) -> dict:
    """Build the standard QB scoring card dict from a merged entry."""
    yards_row = entry.get("yards")
    td_row    = entry.get("tds")
    return {
        "qb_name":               entry["qb_name"],
        "team":                  entry["team"],
        "opponent":              entry["opponent"],
        "week":                  entry["week"],
        "qpyi_score":            yards_row.qpyi_score if yards_row else None,
        "qpyi_grade":            yards_row.grade      if yards_row else None,
        "projected_yards":       yards_row.projected_value if yards_row else None,
        "prop_passing_yards_line": yards_row.prop_line if yards_row else None,
        "passing_yards_edge":    yards_row.edge        if yards_row else None,
        "qtdi_score":            td_row.qtdi_score     if td_row    else None,
        "qtdi_grade":            td_row.grade          if td_row    else None,
        "projected_tds":         td_row.projected_value if td_row   else None,
        "prop_td_line":          td_row.prop_line       if td_row   else None,
        "td_edge":               td_row.edge            if td_row   else None,
        "signal_tag":            (td_row or yards_row).signal_tag
                                 if (td_row or yards_row) else None,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nfl/qbs/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nfl/qbs/today", tags=["NFL"])
async def nfl_qbs_today(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Full list of all scored QBs for the current NFL week.
    Returns QPYI, QTDI, grades, prop lines, projections, and edge for every QB.
    """
    today       = date.today()
    season_year = _nfl_season_year_routes(today)
    week        = await _get_current_nfl_week(db, season_year)

    stmt = (
        select(NFLModelOutputDaily)
        .where(
            and_(
                NFLModelOutputDaily.season_year == season_year,
                NFLModelOutputDaily.week        == week,
            )
        )
        .order_by(NFLModelOutputDaily.qb_name)
    )
    rows = (await db.execute(stmt)).scalars().all()

    if not rows:
        return {
            "week":    week,
            "season":  season_year,
            "count":   0,
            "qbs":     [],
            "message": (
                "No NFL QB data found for this week. "
                "The pipeline has not run yet or no games are scheduled."
            ),
        }

    merged = _merge_nfl_rows(rows)
    cards  = [_build_qb_card(e) for e in merged]

    return {
        "week":        week,
        "season":      season_year,
        "generated_at": datetime.utcnow().isoformat(),
        "count":        len(cards),
        "qbs":          cards,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nfl/rankings/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nfl/rankings/today", tags=["NFL"])
async def nfl_rankings_today(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Ranked list of all QBs for the current NFL week.
    Sorted by composite edge: (passing_yards_edge * 0.5) + (td_edge * 0.5) descending.
    Highest composite edge = strongest combined yards + TD signal at the top.
    """
    today       = date.today()
    season_year = _nfl_season_year_routes(today)
    week        = await _get_current_nfl_week(db, season_year)

    stmt = (
        select(NFLModelOutputDaily)
        .where(
            and_(
                NFLModelOutputDaily.season_year == season_year,
                NFLModelOutputDaily.week        == week,
            )
        )
    )
    rows = (await db.execute(stmt)).scalars().all()

    if not rows:
        return {
            "week":     week,
            "season":   season_year,
            "count":    0,
            "rankings": [],
            "message":  "No NFL QB data found for this week. Pipeline has not run yet.",
        }

    merged = _merge_nfl_rows(rows)
    cards  = [_build_qb_card(e) for e in merged]

    def _composite_edge(card: dict) -> float:
        ye = card.get("passing_yards_edge") or 0.0
        te = card.get("td_edge")            or 0.0
        return (ye * 0.5) + (te * 0.5)

    cards.sort(key=_composite_edge, reverse=True)

    ranked = []
    for rank, card in enumerate(cards, start=1):
        ranked.append({
            "rank":             rank,
            "composite_edge":   round(_composite_edge(card), 2),
            **card,
        })

    return {
        "week":         week,
        "season":       season_year,
        "generated_at": datetime.utcnow().isoformat(),
        "count":        len(ranked),
        "rankings":     ranked,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nfl/qbs/{qb_name}/profile
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nfl/qbs/{qb_name}/profile", tags=["NFL"])
async def nfl_qb_profile(
    qb_name: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Deep-dive on one NFL QB for the current week.
    Returns the full scoring card plus all 12 individual block scores.
    QB name is case-insensitive and URL-decoded automatically by FastAPI.
    """
    today       = date.today()
    season_year = _nfl_season_year_routes(today)
    week        = await _get_current_nfl_week(db, season_year)

    # Case-insensitive name match using ILIKE
    output_stmt = (
        select(NFLModelOutputDaily)
        .where(
            and_(
                NFLModelOutputDaily.season_year == season_year,
                NFLModelOutputDaily.week        == week,
                NFLModelOutputDaily.qb_name.ilike(qb_name),
            )
        )
    )
    output_rows = (await db.execute(output_stmt)).scalars().all()

    if not output_rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No NFL data found for QB '{qb_name}' in week {week} "
                f"of the {season_year} season. "
                "Check the spelling or confirm the pipeline has run for this week."
            ),
        )

    # Merge the two market rows into one card
    merged = _merge_nfl_rows(output_rows)
    card   = _build_qb_card(merged[0])

    # Pull the feature block scores
    feat_stmt = (
        select(NFLQBFeaturesDaily)
        .where(
            and_(
                NFLQBFeaturesDaily.season_year == season_year,
                NFLQBFeaturesDaily.week        == week,
                NFLQBFeaturesDaily.qb_name.ilike(qb_name),
            )
        )
        .limit(1)
    )
    feat_row = (await db.execute(feat_stmt)).scalar_one_or_none()

    block_scores = {
        "osw_score":    feat_row.osw_score    if feat_row else None,
        "qsr_score":    feat_row.qsr_score    if feat_row else None,
        "gsp_score":    feat_row.gsp_score    if feat_row else None,
        "scb_score":    feat_row.scb_score    if feat_row else None,
        "pdr_score":    feat_row.pdr_score    if feat_row else None,
        "ens_score":    feat_row.ens_score    if feat_row else None,
        "dsr_score":    feat_row.dsr_score    if feat_row else None,
        "rct_score":    feat_row.rct_score    if feat_row else None,
        "ord_score":    feat_row.ord_score    if feat_row else None,
        "qtr_score":    feat_row.qtr_score    if feat_row else None,
        "gsp_td_score": feat_row.gsp_td_score if feat_row else None,
        "scb_td_score": feat_row.scb_td_score if feat_row else None,
    }

    return {
        "week":         week,
        "season":       season_year,
        "generated_at": datetime.utcnow().isoformat(),
        **card,
        "block_scores": block_scores,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nfl/exports/weekly.csv
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nfl/exports/weekly.csv", tags=["NFL"])
async def nfl_export_weekly_csv(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Download the current NFL week's full QB output as a spreadsheet-ready CSV.
    Filename format: axiom_nfl_week_{week}_{YYYY-MM-DD}.csv
    """
    today       = date.today()
    season_year = _nfl_season_year_routes(today)
    week        = await _get_current_nfl_week(db, season_year)

    stmt = (
        select(NFLModelOutputDaily)
        .where(
            and_(
                NFLModelOutputDaily.season_year == season_year,
                NFLModelOutputDaily.week        == week,
            )
        )
        .order_by(NFLModelOutputDaily.qb_name)
    )
    rows = (await db.execute(stmt)).scalars().all()

    filename = f"axiom_nfl_week_{week}_{today}.csv"

    if not rows:
        return Response(
            content="qb_name,team,week\nNo data for this week.\n",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    merged     = _merge_nfl_rows(rows)
    export_rows = []
    for entry in merged:
        card = _build_qb_card(entry)
        export_rows.append({
            "week":                    card["week"],
            "season":                  season_year,
            "qb_name":                 card["qb_name"],
            "team":                    card["team"],
            "opponent":                card["opponent"],
            "qpyi_score":              card["qpyi_score"]              or "",
            "qpyi_grade":              card["qpyi_grade"]              or "",
            "projected_yards":         round(card["projected_yards"], 1)
                                       if card["projected_yards"] is not None else "",
            "prop_passing_yards_line": card["prop_passing_yards_line"] or "",
            "passing_yards_edge":      round(card["passing_yards_edge"], 2)
                                       if card["passing_yards_edge"] is not None else "",
            "qtdi_score":              card["qtdi_score"]              or "",
            "qtdi_grade":              card["qtdi_grade"]              or "",
            "projected_tds":           round(card["projected_tds"], 2)
                                       if card["projected_tds"] is not None else "",
            "prop_td_line":            card["prop_td_line"]            or "",
            "td_edge":                 round(card["td_edge"], 2)
                                       if card["td_edge"] is not None else "",
            "signal_tag":              card["signal_tag"]              or "",
        })

    csv_content = rows_to_csv(export_rows)

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/tasks/nfl/run-weekly  (protected by internal token)
# ─────────────────────────────────────────────────────────────

@router.post("/v1/tasks/nfl/run-weekly", tags=["Tasks"])
async def run_nfl_weekly_task(
    background_tasks: BackgroundTasks,
    body: NFLRunWeeklyRequest = Body(default=NFLRunWeeklyRequest()),
    axiom_internal_token: Optional[str] = Header(
        default=None, alias="AXIOM-INTERNAL-TOKEN"
    ),
):
    """
    Trigger the NFL weekly QB scoring pipeline.
    Returns immediately — pipeline runs in the background.
    Results are saved to the database; read them via GET /v1/nfl/qbs/today.
    Protected: requires AXIOM-INTERNAL-TOKEN header.
    """
    if axiom_internal_token != settings.AXIOM_INTERNAL_TOKEN:
        raise HTTPException(
            status_code=403, detail="Invalid or missing AXIOM-INTERNAL-TOKEN."
        )

    dry_run = body.dry_run
    log.info("NFL pipeline triggered",
             timestamp=datetime.utcnow().isoformat(), dry_run=dry_run)

    async def _run_nfl_pipeline_task() -> None:
        from app.tasks.nfl_pipeline import run_nfl_pipeline
        try:
            result = await run_nfl_pipeline(dry_run=dry_run)
            log.info("NFL background pipeline complete",
                     status=result.get("status"),
                     qbs_scored=result.get("qbs_scored"),
                     elapsed=result.get("elapsed_seconds"))
        except Exception as exc:
            log.error("NFL background pipeline failed", error=str(exc))

    background_tasks.add_task(_run_nfl_pipeline_task)

    return {
        "status":   "started",
        "dry_run":  dry_run,
        "message": (
            "NFL pipeline is running in the background. "
            "Results will be available at GET /v1/nfl/qbs/today once complete."
        ),
    }


# ── NHL Endpoints ──────────────────────────────────────────


class NHLRunDailyRequest(BaseModel):
    dry_run: bool = False
    game_date: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# NHL shared helpers
# ─────────────────────────────────────────────────────────────

def _build_nhl_card(row: NHLModelOutputDaily) -> dict:
    """Build the standard NHL scoring card dict from one output row."""
    return {
        "player_name":              row.player_name,
        "team":                     row.team,
        "opponent":                 row.opponent,
        "position":                 row.position,
        "market":                   row.market,
        "gsai_score":               row.gsai_score,
        "ppsi_score":               row.ppsi_score,
        "grade":                    row.grade,
        "projected_value":          row.projected_value,
        "prop_line":                row.prop_line,
        "edge":                     row.edge,
        "signal_tag":               row.signal_tag,
        "ml_projection":            row.ml_projection,
        "ml_signal":                row.ml_signal,
        "playoff_discount_applied": row.playoff_discount_applied,
    }


async def _nhl_rows_for_date(db: AsyncSession, query_date: date) -> list:
    """Return all NHLModelOutputDaily rows for a given date, joined through NHLGame."""
    stmt = (
        select(NHLModelOutputDaily)
        .join(NHLGame, NHLModelOutputDaily.game_id == NHLGame.game_id)
        .where(NHLGame.game_date == query_date)
        .order_by(NHLModelOutputDaily.player_name, NHLModelOutputDaily.market)
    )
    return (await db.execute(stmt)).scalars().all()


# ─────────────────────────────────────────────────────────────
# GET /v1/nhl/players/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nhl/players/today", tags=["NHL"])
async def nhl_players_today(
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Full list of all scored NHL players for a given date.

    Returns one scoring card per player per market.
    Goalies have one card (market: shots_faced).
    Skaters have four cards (markets: points, goals, assists, shots_on_goal).
    Response is separated into { "goalies": [...], "skaters": [...] }.
    """
    query_date = target_date or _today_eastern()
    rows = await _nhl_rows_for_date(db, query_date)

    if not rows:
        return {
            "date":     str(query_date),
            "goalies":  [],
            "skaters":  [],
            "message": (
                "No NHL player data found for this date. "
                "The pipeline has not run yet or no games are scheduled."
            ),
        }

    goalies = [_build_nhl_card(r) for r in rows if r.market == "shots_faced"]
    skaters = [_build_nhl_card(r) for r in rows if r.market != "shots_faced"]

    return {
        "date":           str(query_date),
        "generated_at":   datetime.utcnow().isoformat(),
        "goalie_count":   len({c["player_name"] for c in goalies}),
        "skater_count":   len({c["player_name"] for c in skaters}),
        "goalies":        goalies,
        "skaters":        skaters,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nhl/rankings/today
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nhl/rankings/today", tags=["NHL"])
async def nhl_rankings_today(
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Ranked NHL player list for a given date.

    Goalies are ranked by projected shots edge (shots_faced market), best edge first.
    Skaters are ranked by projected points edge (points market), best edge first.
    Each entry includes a rank field starting at 1 within each group.
    """
    query_date = target_date or date.today()
    rows = await _nhl_rows_for_date(db, query_date)

    if not rows:
        return {
            "date":     str(query_date),
            "goalies":  [],
            "skaters":  [],
            "message":  "No NHL player data found for this date. Pipeline has not run yet.",
        }

    # Rank goalies by shots_faced edge, descending
    goalie_rows = [r for r in rows if r.market == "shots_faced"]
    goalie_rows.sort(key=lambda r: (r.edge or 0.0), reverse=True)
    goalies_ranked = []
    for rank, r in enumerate(goalie_rows, start=1):
        card = _build_nhl_card(r)
        card["rank"] = rank
        goalies_ranked.append(card)

    # Rank skaters by points edge, descending
    skater_rows = [r for r in rows if r.market == "points"]
    skater_rows.sort(key=lambda r: (r.edge or 0.0), reverse=True)
    skaters_ranked = []
    for rank, r in enumerate(skater_rows, start=1):
        card = _build_nhl_card(r)
        card["rank"] = rank
        skaters_ranked.append(card)

    return {
        "date":           str(query_date),
        "generated_at":   datetime.utcnow().isoformat(),
        "goalie_count":   len(goalies_ranked),
        "skater_count":   len(skaters_ranked),
        "goalies":        goalies_ranked,
        "skaters":        skaters_ranked,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nhl/players/{player_name}/profile
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nhl/players/{player_name}/profile", tags=["NHL"])
async def nhl_player_profile(
    player_name: str,
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Deep-dive on one NHL player for a given date.

    Returns the full scoring card for all markets plus all 6 individual block scores
    from the GSAI or PPSI engine depending on position.
    Player name is case-insensitive. Returns 404 if the player has no data today.
    """
    query_date = target_date or date.today()

    # Load all market rows for this player using case-insensitive name match
    stmt = (
        select(NHLModelOutputDaily)
        .join(NHLGame, NHLModelOutputDaily.game_id == NHLGame.game_id)
        .where(
            and_(
                NHLGame.game_date == query_date,
                NHLModelOutputDaily.player_name.ilike(player_name),
            )
        )
        .order_by(NHLModelOutputDaily.market)
    )
    output_rows = (await db.execute(stmt)).scalars().all()

    if not output_rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No NHL data found for '{player_name}' on {query_date}. "
                "Check the spelling or confirm the pipeline has run for this date."
            ),
        )

    first = output_rows[0]
    is_goalie = first.market == "shots_faced"
    game_id   = first.game_id
    player_id = first.player_id

    cards = [_build_nhl_card(r) for r in output_rows]

    # Pull engine block scores from the appropriate features table
    block_scores: dict = {}
    if is_goalie:
        feat_stmt = (
            select(NHLGoalieFeaturesDaily)
            .where(
                and_(
                    NHLGoalieFeaturesDaily.game_id   == game_id,
                    NHLGoalieFeaturesDaily.player_id == player_id,
                )
            )
            .limit(1)
        )
        feat = (await db.execute(feat_stmt)).scalar_one_or_none()
        if feat:
            block_scores = {
                "gsai_score": feat.gsai_score,
                "gss_score":  feat.gss_score,   # Goalie Save Suppression    29%
                "osq_score":  feat.osq_score,   # Opponent Shooting Quality  24%
                "top_score":  feat.top_score,   # Tactical / Operational     18%
                "gen_score":  feat.gen_score,   # Game Environment           16%
                "rfs_score":  feat.rfs_score,   # Referee Flow Score          8%
                "tsc_score":  feat.tsc_score,   # Team Structure & Coverage   5%
                "projected_shots": feat.projected_shots,
            }
    else:
        feat_stmt = (
            select(NHLSkaterFeaturesDaily)
            .where(
                and_(
                    NHLSkaterFeaturesDaily.game_id   == game_id,
                    NHLSkaterFeaturesDaily.player_id == player_id,
                )
            )
            .limit(1)
        )
        feat = (await db.execute(feat_stmt)).scalar_one_or_none()
        if feat:
            block_scores = {
                "ppsi_score":        feat.ppsi_score,
                "osr_score":         feat.osr_score,   # Opponent Scoring Resistance 28%
                "pmr_score":         feat.pmr_score,   # Player Matchup Rating       22%
                "per_score":         feat.per_score,   # Player Efficiency Rating    18%
                "pop_score":         feat.pop_score,   # Points Operational          14%
                "rps_score":         feat.rps_score,   # Referee PP Score            10%
                "tld_score":         feat.tld_score,   # Top-Line Deployment          8%
                "projected_pts":     feat.projected_pts,
                "projected_sog":     feat.projected_sog,
                "projected_goals":   feat.projected_goals,
                "projected_assists": feat.projected_assists,
            }

    return {
        "date":         str(query_date),
        "generated_at": datetime.utcnow().isoformat(),
        "player_name":  first.player_name,
        "team":         first.team,
        "opponent":     first.opponent,
        "position":     first.position,
        "player_type":  "goalie" if is_goalie else "skater",
        "markets":      cards,
        "block_scores": block_scores,
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/nhl/exports/daily.csv
# ─────────────────────────────────────────────────────────────

@router.get("/v1/nhl/exports/daily.csv", tags=["NHL"])
async def nhl_export_daily_csv(
    target_date: Optional[date] = Query(
        default=None, description="Date (YYYY-MM-DD). Defaults to today."
    ),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """
    Download today's full NHL output as a spreadsheet-ready CSV file.
    Includes both goalies and skaters in one file.
    A position_group column ("goalie" or "skater") distinguishes them.
    Filename format: axiom_nhl_{date}.csv
    """
    query_date = target_date or date.today()
    filename   = f"axiom_nhl_{query_date}.csv"

    rows = await _nhl_rows_for_date(db, query_date)

    if not rows:
        return Response(
            content="date,player_name,position_group\nNo data for this date.\n",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    export_rows = []
    for r in rows:
        export_rows.append({
            "date":                     str(query_date),
            "position_group":           "goalie" if r.market == "shots_faced" else "skater",
            "player_name":              r.player_name,
            "team":                     r.team,
            "opponent":                 r.opponent,
            "position":                 r.position,
            "market":                   r.market,
            "gsai_score":               round(r.gsai_score, 2) if r.gsai_score is not None else "",
            "ppsi_score":               round(r.ppsi_score, 2) if r.ppsi_score is not None else "",
            "grade":                    r.grade or "",
            "projected_value":          round(r.projected_value, 2),
            "prop_line":                r.prop_line if r.prop_line is not None else "",
            "edge":                     round(r.edge, 2) if r.edge is not None else "",
            "signal_tag":               r.signal_tag or "",
            "ml_projection":            round(r.ml_projection, 2) if r.ml_projection is not None else "",
            "ml_signal":                r.ml_signal or "",
            "playoff_discount_applied": r.playoff_discount_applied,
        })

    csv_content = rows_to_csv(export_rows)

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/tasks/nhl/run-daily  (protected by internal token)
# ─────────────────────────────────────────────────────────────

@router.post("/v1/tasks/nhl/run-daily", tags=["Tasks"])
async def run_nhl_daily_task(
    background_tasks: BackgroundTasks,
    body: NHLRunDailyRequest = Body(default=NHLRunDailyRequest()),
    axiom_internal_token: Optional[str] = Header(
        default=None, alias="AXIOM-INTERNAL-TOKEN"
    ),
):
    """
    Trigger the NHL daily player scoring pipeline.
    Returns immediately — pipeline runs in the background.
    Results are saved to the database; read them via GET /v1/nhl/players/today.
    Protected: requires AXIOM-INTERNAL-TOKEN header.
    """
    if axiom_internal_token != settings.AXIOM_INTERNAL_TOKEN:
        raise HTTPException(
            status_code=403, detail="Invalid or missing AXIOM-INTERNAL-TOKEN."
        )

    dry_run   = body.dry_run
    game_date = body.game_date  # None = today inside the pipeline
    log.info(
        "NHL pipeline triggered",
        timestamp=datetime.utcnow().isoformat(),
        game_date=game_date or "today",
        dry_run=dry_run,
    )

    async def _run_nhl_pipeline_task() -> None:
        from app.tasks.nhl_pipeline import run_nhl_pipeline
        try:
            result = await run_nhl_pipeline(game_date=game_date, dry_run=dry_run)
            log.info(
                "NHL background pipeline complete",
                status=result.get("status"),
                goalies_scored=result.get("goalies_scored"),
                skaters_scored=result.get("skaters_scored"),
                elapsed=result.get("elapsed_seconds"),
            )
        except Exception as exc:
            log.error("NHL background pipeline failed", error=str(exc))

    background_tasks.add_task(_run_nhl_pipeline_task)

    return {
        "status":    "started",
        "game_date": game_date or str(_today_eastern()),
        "dry_run":   dry_run,
        "message": (
            "NHL pipeline is running in the background. "
            "Results will be available at GET /v1/nhl/players/today once complete."
        ),
    }
