"""
app/ml/trainer.py — Orchestrates the full ML training and prediction cycle.

This runs as part of the daily pipeline AFTER the formula engine has scored
all pitchers. It does three things in sequence:

  1. COLLECT — Pull today's formula outputs + features and store them as
               pending training samples (actual_hits / actual_ks = None).

  2. LABEL   — For any pending samples from previous days, check if the game
               is complete and fill in the actual hits/Ks from the MLB boxscore.
               This is the feedback loop that makes the model learn.

  3. TRAIN & PREDICT — Re-train the ML engine on ALL labeled samples,
                        then predict for today's pitchers and store the results.

The trainer is designed to be called once per day. It is idempotent — calling
it multiple times on the same day is safe.
"""
import asyncio
from datetime import date, timedelta
from typing import Optional

import httpx
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ml.engine import AxiomMLEngine
from app.ml.scorer import convert_ml_predictions
from app.models.models import MLTrainingSample
from app.utils.logging import get_logger

log = get_logger("ml_trainer")

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# ── Collect: insert or update one training sample per pitcher per day
_COLLECT_SQL = text("""
    INSERT INTO ml_training_samples (
        pitcher_id, game_id, game_date,
        owc_score, pcs_score, ens_score, ops_score, uhs_score, dsc_score,
        ocr_score, pmr_score, per_score, kop_score, uks_score, tlr_score,
        season_h9, season_k9, expected_ip,
        bullpen_fatigue_opp, bullpen_fatigue_own,
        ens_park, ens_temp, ens_air,
        formula_husi, formula_kusi, formula_proj_hits, formula_proj_ks,
        catcher_strike_rate, tfi_rest_hours, tfi_tz_shift, vaa_degrees, extension_ft,
        actual_hits, actual_ks, actual_ip, is_complete
    ) VALUES (
        :pitcher_id, :game_id, :game_date,
        :owc_score, :pcs_score, :ens_score, :ops_score, :uhs_score, :dsc_score,
        :ocr_score, :pmr_score, :per_score, :kop_score, :uks_score, :tlr_score,
        :season_h9, :season_k9, :expected_ip,
        :bullpen_fatigue_opp, :bullpen_fatigue_own,
        :ens_park, :ens_temp, :ens_air,
        :formula_husi, :formula_kusi, :formula_proj_hits, :formula_proj_ks,
        :catcher_strike_rate, :tfi_rest_hours, :tfi_tz_shift, :vaa_degrees, :extension_ft,
        :actual_hits, :actual_ks, :actual_ip, :is_complete
    )
    ON CONFLICT (pitcher_id, game_id) DO UPDATE SET
        owc_score          = EXCLUDED.owc_score,
        pcs_score          = EXCLUDED.pcs_score,
        ens_score          = EXCLUDED.ens_score,
        ops_score          = EXCLUDED.ops_score,
        uhs_score          = EXCLUDED.uhs_score,
        dsc_score          = EXCLUDED.dsc_score,
        ocr_score          = EXCLUDED.ocr_score,
        pmr_score          = EXCLUDED.pmr_score,
        per_score          = EXCLUDED.per_score,
        kop_score          = EXCLUDED.kop_score,
        uks_score          = EXCLUDED.uks_score,
        tlr_score          = EXCLUDED.tlr_score,
        formula_husi       = EXCLUDED.formula_husi,
        formula_kusi       = EXCLUDED.formula_kusi,
        formula_proj_hits  = EXCLUDED.formula_proj_hits,
        formula_proj_ks    = EXCLUDED.formula_proj_ks
""")

# ── Store ML predictions — one row per pitcher per game
_OUTPUT_SQL = text("""
    INSERT INTO ml_model_outputs (
        pitcher_id, game_id, game_date,
        ml_proj_hits, ml_proj_ks,
        ml_husi, ml_kusi, ml_husi_grade, ml_kusi_grade,
        husi_delta, kusi_delta,
        husi_divergence, kusi_divergence,
        consensus_husi_grade, consensus_kusi_grade,
        model_version, training_samples, mae_hits, mae_ks
    ) VALUES (
        :pitcher_id, :game_id, :game_date,
        :ml_proj_hits, :ml_proj_ks,
        :ml_husi, :ml_kusi, :ml_husi_grade, :ml_kusi_grade,
        :husi_delta, :kusi_delta,
        :husi_divergence, :kusi_divergence,
        :consensus_husi_grade, :consensus_kusi_grade,
        :model_version, :training_samples, :mae_hits, :mae_ks
    )
    ON CONFLICT (pitcher_id, game_id) DO UPDATE SET
        ml_proj_hits         = EXCLUDED.ml_proj_hits,
        ml_proj_ks           = EXCLUDED.ml_proj_ks,
        ml_husi              = EXCLUDED.ml_husi,
        ml_kusi              = EXCLUDED.ml_kusi,
        ml_husi_grade        = EXCLUDED.ml_husi_grade,
        ml_kusi_grade        = EXCLUDED.ml_kusi_grade,
        husi_delta           = EXCLUDED.husi_delta,
        kusi_delta           = EXCLUDED.kusi_delta,
        husi_divergence      = EXCLUDED.husi_divergence,
        kusi_divergence      = EXCLUDED.kusi_divergence,
        consensus_husi_grade = EXCLUDED.consensus_husi_grade,
        consensus_kusi_grade = EXCLUDED.consensus_kusi_grade,
        model_version        = EXCLUDED.model_version,
        training_samples     = EXCLUDED.training_samples,
        mae_hits             = EXCLUDED.mae_hits,
        mae_ks               = EXCLUDED.mae_ks
""")


class MLTrainer:
    """
    Orchestrates the ML training cycle. Called once per day by the pipeline.
    """

    def __init__(self) -> None:
        pass

    # ─────────────────────────────────────────────────────────
    # Step 1: Collect — store today's samples as pending
    # ─────────────────────────────────────────────────────────

    async def collect_today(
        self,
        session: AsyncSession,
        today: date,
        scored_pitchers: list[dict],
    ) -> int:
        """
        Store today's scored pitchers as pending ML training samples.

        Each row has all the feature scores + formula predictions,
        but actual_hits / actual_ks are NULL until the game finishes.

        Returns the number of samples inserted/updated.
        """
        if not scored_pitchers:
            return 0

        count = 0
        for p in scored_pitchers:
            params = {
                "pitcher_id":           p.get("pitcher_id"),
                "game_id":              p.get("game_id"),
                "game_date":            today,
                # Formula block scores
                "owc_score":            p.get("owc_score"),
                "pcs_score":            p.get("pcs_score"),
                "ens_score":            p.get("ens_score"),
                "ops_score":            p.get("ops_score"),
                "uhs_score":            p.get("uhs_score"),
                "dsc_score":            p.get("dsc_score"),
                "ocr_score":            p.get("ocr_score"),
                "pmr_score":            p.get("pmr_score"),
                "per_score":            p.get("per_score"),
                "kop_score":            p.get("kop_score"),
                "uks_score":            p.get("uks_score"),
                "tlr_score":            p.get("tlr_score"),
                # Raw stats — pipeline uses season_hits_per_9 / season_k_per_9
                "season_h9":            p.get("season_h9") or p.get("season_hits_per_9"),
                "season_k9":            p.get("season_k9") or p.get("season_k_per_9"),
                "expected_ip":          p.get("expected_ip"),
                "bullpen_fatigue_opp":  p.get("bullpen_fatigue_opp") or 0.0,
                "bullpen_fatigue_own":  p.get("bullpen_fatigue_own") or 0.0,
                "ens_park":             p.get("ens_park"),
                "ens_temp":             p.get("ens_temp"),
                "ens_air":              p.get("ens_air"),
                # Formula outputs — pipeline uses husi/kusi/projected_hits/projected_ks
                "formula_husi":         p.get("formula_husi") or p.get("husi"),
                "formula_kusi":         p.get("formula_kusi") or p.get("kusi"),
                "formula_proj_hits":    p.get("formula_proj_hits") or p.get("projected_hits"),
                "formula_proj_ks":      p.get("formula_proj_ks") or p.get("projected_ks"),
                # Hidden variables
                "catcher_strike_rate":  p.get("catcher_strike_rate"),
                "tfi_rest_hours":       p.get("tfi_rest_hours"),
                "tfi_tz_shift":         p.get("tfi_tz_shift"),
                "vaa_degrees":          p.get("vaa_degrees"),
                "extension_ft":         p.get("extension_ft"),
                # Actual outcomes — NULL until the game completes
                "actual_hits":          None,
                "actual_ks":            None,
                "actual_ip":            None,
                "is_complete":          False,
            }
            await session.execute(_COLLECT_SQL, params)
            count += 1

        log.info("ML samples collected", date=str(today), count=count)
        return count

    # ─────────────────────────────────────────────────────────
    # Step 2: Label — fill in actual outcomes for finished games
    # ─────────────────────────────────────────────────────────

    async def label_completed_games(
        self,
        session: AsyncSession,
        lookback_days: int = 7,
    ) -> int:
        """
        For any pending samples from the last N days, check if the game is
        complete and fill in the actual hits/Ks from the MLB boxscore.

        Returns the number of samples newly labeled.
        """
        cutoff = date.today() - timedelta(days=lookback_days)
        stmt = select(MLTrainingSample).where(
            MLTrainingSample.is_complete == False,
            MLTrainingSample.game_date >= cutoff,
        )
        result = await session.execute(stmt)
        pending = result.scalars().all()

        if not pending:
            log.info("ML label: no pending samples")
            return 0

        log.info("ML label: checking pending samples", count=len(pending))
        labeled = 0

        async with httpx.AsyncClient() as client:
            game_ids = list({s.game_id for s in pending})
            boxscores = {}
            for gid in game_ids:
                bs = await self._fetch_boxscore(client, gid)
                if bs:
                    boxscores[gid] = bs

        for sample in pending:
            bs = boxscores.get(sample.game_id)
            if not bs:
                continue

            actuals = self._extract_pitcher_actuals(bs, sample.pitcher_id)
            if actuals is None:
                continue

            await session.execute(
                update(MLTrainingSample)
                .where(
                    MLTrainingSample.pitcher_id == sample.pitcher_id,
                    MLTrainingSample.game_id == sample.game_id,
                )
                .values(
                    actual_hits=actuals["hits"],
                    actual_ks=actuals["ks"],
                    actual_ip=actuals["ip"],
                    is_complete=True,
                )
            )
            labeled += 1

        log.info("ML label: samples newly labeled", count=labeled)
        return labeled

    # ─────────────────────────────────────────────────────────
    # Step 3: Train and predict
    # ─────────────────────────────────────────────────────────

    async def train_and_predict(
        self,
        session: AsyncSession,
        today: date,
        today_samples: list[dict],
        formula_outputs: dict[str, dict],
    ) -> dict:
        """
        Train the ML engine on all historical labeled data, then predict
        for today's pitchers and store the ML outputs.

        Returns a summary dict: {"trained": bool, "n_samples": int, ...}
        """
        # Load ALL labeled training samples from the database
        stmt = select(MLTrainingSample).where(MLTrainingSample.is_complete == True)
        result = await session.execute(stmt)
        all_labeled = result.scalars().all()

        training_data = [
            {
                "pitcher_id":          s.pitcher_id,
                "game_id":             s.game_id,
                "owc_score":           s.owc_score,
                "pcs_score":           s.pcs_score,
                "ens_score":           s.ens_score,
                "ops_score":           s.ops_score,
                "uhs_score":           s.uhs_score,
                "dsc_score":           s.dsc_score,
                "ocr_score":           s.ocr_score,
                "pmr_score":           s.pmr_score,
                "per_score":           s.per_score,
                "kop_score":           s.kop_score,
                "uks_score":           s.uks_score,
                "tlr_score":           s.tlr_score,
                "season_h9":           s.season_h9,
                "season_k9":           s.season_k9,
                "expected_ip":         s.expected_ip,
                "bullpen_fatigue_opp": s.bullpen_fatigue_opp,
                "bullpen_fatigue_own": s.bullpen_fatigue_own,
                "ens_park":            s.ens_park,
                "ens_temp":            s.ens_temp,
                "ens_air":             s.ens_air,
                "formula_husi":        s.formula_husi,
                "formula_kusi":        s.formula_kusi,
                "formula_proj_hits":   s.formula_proj_hits,
                "formula_proj_ks":     s.formula_proj_ks,
                "catcher_strike_rate": s.catcher_strike_rate,
                "tfi_rest_hours":      s.tfi_rest_hours,
                "tfi_tz_shift":        s.tfi_tz_shift,
                "vaa_degrees":         s.vaa_degrees,
                "extension_ft":        s.extension_ft,
                "actual_hits":         s.actual_hits,
                "actual_ks":           s.actual_ks,
            }
            for s in all_labeled
        ]

        # Train the engine
        ml_engine = AxiomMLEngine()
        train_result = ml_engine.train(training_data)

        if not train_result["trained"]:
            log.info("ML predictions skipped this run", reason=train_result.get("reason"))
            return {"trained": False, "ml_outputs": [], **train_result}

        # Remap pipeline-format keys to feature-format keys before predicting.
        # The pipeline dict uses husi/kusi/projected_hits/season_hits_per_9;
        # features.py looks for formula_husi/formula_kusi/formula_proj_hits/season_h9.
        predict_samples = []
        for s in today_samples:
            ps = dict(s)
            if ps.get("formula_husi") is None:
                ps["formula_husi"] = s.get("husi")
            if ps.get("formula_kusi") is None:
                ps["formula_kusi"] = s.get("kusi")
            if ps.get("formula_proj_hits") is None:
                ps["formula_proj_hits"] = s.get("projected_hits")
            if ps.get("formula_proj_ks") is None:
                ps["formula_proj_ks"] = s.get("projected_ks")
            if ps.get("season_h9") is None:
                ps["season_h9"] = s.get("season_hits_per_9")
            if ps.get("season_k9") is None:
                ps["season_k9"] = s.get("season_k_per_9")
            predict_samples.append(ps)

        raw_predictions = ml_engine.predict(predict_samples)
        ml_outputs = convert_ml_predictions(raw_predictions, formula_outputs)

        # Store ML outputs
        stored = 0
        for o in ml_outputs:
            params = {
                "pitcher_id":           o["pitcher_id"],
                "game_id":              o["game_id"],
                "game_date":            today,
                "ml_proj_hits":         o["ml_proj_hits"],
                "ml_proj_ks":           o["ml_proj_ks"],
                "ml_husi":              o["ml_husi"],
                "ml_kusi":              o["ml_kusi"],
                "ml_husi_grade":        o["ml_husi_grade"],
                "ml_kusi_grade":        o["ml_kusi_grade"],
                "husi_delta":           o["husi_delta"],
                "kusi_delta":           o["kusi_delta"],
                "husi_divergence":      o["husi_divergence"],
                "kusi_divergence":      o["kusi_divergence"],
                "consensus_husi_grade": o["consensus_husi_grade"],
                "consensus_kusi_grade": o["consensus_kusi_grade"],
                "model_version":        o["model_version"],
                "training_samples":     o["training_samples"],
                "mae_hits":             o["mae_hits"],
                "mae_ks":               o["mae_ks"],
            }
            await session.execute(_OUTPUT_SQL, params)
            stored += 1

        log.info("ML outputs stored", count=stored, version=train_result["version"])

        return {
            "trained": True,
            "n_samples": train_result["n_samples"],
            "mae_hits": train_result["mae_hits"],
            "mae_ks": train_result["mae_ks"],
            "version": train_result["version"],
            "ml_outputs": ml_outputs,
        }

    # ─────────────────────────────────────────────────────────
    # MLB API helpers
    # ─────────────────────────────────────────────────────────

    async def _fetch_boxscore(
        self,
        client: httpx.AsyncClient,
        game_id: str,
    ) -> Optional[dict]:
        """Fetch the boxscore for a finished game. Returns None if not complete."""
        try:
            url = f"{MLB_BASE}/game/{game_id}/boxscore"
            resp = await client.get(url, timeout=15.0)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("ML boxscore fetch failed", game_id=game_id, error=str(exc))
            return None

    @staticmethod
    def _extract_pitcher_actuals(
        boxscore: dict,
        pitcher_id: str,
    ) -> Optional[dict]:
        """
        Extract actual hits allowed, Ks, and IP for a specific starting pitcher.
        Returns None if the pitcher is not found or the game is not complete.
        """
        teams = boxscore.get("teams", {})
        for side in ("home", "away"):
            side_data = teams.get(side, {})
            players = side_data.get("players", {})
            player_key = f"ID{pitcher_id}"

            if player_key not in players:
                continue

            player = players[player_key]
            stats = player.get("stats", {}).get("pitching", {})

            if not stats:
                continue

            ip_str = str(stats.get("inningsPitched") or "0")
            try:
                parts = ip_str.split(".")
                ip = int(parts[0]) + (int(parts[1]) / 3 if len(parts) > 1 else 0)
            except (ValueError, IndexError):
                ip = 0.0

            if ip < 1.0:
                return None

            return {
                "hits": float(stats.get("hits") or 0),
                "ks":   float(stats.get("strikeOuts") or 0),
                "ip":   round(ip, 2),
            }

        return None
