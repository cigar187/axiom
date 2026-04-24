"""
PitcherFeatureSet — the single data contract between data fetchers and the scoring engine.

The scoring engine only reads from this dataclass.
Data fetchers only write to this dataclass.
This hard boundary means you can swap any data source without touching the scoring logic.

All feature values are already-normalized 0-100 scores UNLESS they are raw values
that the scoring engine will normalize internally.

Convention:
  - Fields ending in _score are already 0-100
  - Fields ending in _raw are original stats (pitcher ERA, BABIP, etc.)
  - Fields that are just None will fall back to neutral (50) inside the engine
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PitcherFeatureSet:
    # ── Identity
    pitcher_id: str = ""
    pitcher_name: str = ""
    game_id: str = ""
    team: str = ""
    opponent: str = ""
    handedness: Optional[str] = None

    # ── Operational context
    lineup_confirmed: bool = False
    umpire_confirmed: bool = False
    bullpen_data_available: bool = False
    projected_batters_faced: Optional[float] = None  # for H7 interaction rule

    # ── Prop lines (filled by Rundown fetcher)
    k_line: Optional[float] = None
    k_over_odds: Optional[float] = None
    k_under_odds: Optional[float] = None
    hits_line: Optional[float] = None
    hits_over_odds: Optional[float] = None
    hits_under_odds: Optional[float] = None

    # ── Base stats (filled by MLB Stats API — used for projection formulas)
    season_hits_per_9: Optional[float] = None    # raw hits/9 stat → drives base_hits
    season_k_per_9: Optional[float] = None       # raw K/9 stat → drives base_ks

    # ── Innings pitched context (drives realistic IP window for projections)
    avg_ip_per_start: Optional[float] = None     # this season's actual avg IP/start (IP / GS)
    mlb_service_years: Optional[int] = None      # years in MLB — used as tier fallback when avg_ip unavailable

    # ────────────────────────────────────────────────────────────────────
    # HUSI feature scores (0-100 each, 50 = neutral)
    # ────────────────────────────────────────────────────────────────────

    # OWC — Opponent Weaknesses vs Contact
    owc_babip: Optional[float] = None   # opponent BABIP vs pitcher type
    owc_hh: Optional[float] = None      # opponent hard-hit rate
    owc_bar: Optional[float] = None     # opponent barrel rate
    owc_ld: Optional[float] = None      # opponent line-drive rate
    owc_xba: Optional[float] = None     # opponent expected batting average
    owc_bot3: Optional[float] = None    # bottom-3 hitters weakness score
    owc_topheavy: Optional[float] = None  # top-heavy lineup vs weak contact tail

    # PCS — Pitcher Contact Suppression
    pcs_gb: Optional[float] = None      # ground-ball rate
    pcs_soft: Optional[float] = None    # soft-contact rate
    pcs_bara: Optional[float] = None    # barrel rate against
    pcs_hha: Optional[float] = None     # hard-hit rate against
    pcs_xbaa: Optional[float] = None    # xBA against
    pcs_xwobaa: Optional[float] = None  # xwOBA against
    pcs_cmd: Optional[float] = None     # command score (walk rate)
    pcs_reg: Optional[float] = None     # pitch-to-contact regularity

    # ENS — Environmental Score
    ens_park: Optional[float] = None    # park factor (hits suppression)
    ens_windin: Optional[float] = None  # wind direction benefit
    ens_temp: Optional[float] = None    # temperature effect on ball
    ens_air: Optional[float] = None     # air density / altitude
    ens_roof: Optional[float] = None    # retractable roof benefit
    ens_of: Optional[float] = None      # outfield dimensions
    ens_inf: Optional[float] = None     # infield surface type

    # Park Factor Direct Override
    # Applied as a multiplier on projected_hits AFTER the HUSI formula.
    # Extreme hitter parks (Coors = 1.18+) cannot be captured fully through ENS block weighting alone.
    park_hits_multiplier: Optional[float] = None  # e.g. 1.18 for Coors, 0.90 for Petco
    park_extreme: bool = False                     # True when park_score < 40 (triggers HV9 penalty — Coors, Chase, GABP, etc.)

    # OPS — Operational Score
    ops_pcap: Optional[float] = None    # pitcher pitch count capacity
    ops_hook: Optional[float] = None    # manager hook tendency
    ops_traffic: Optional[float] = None # high-traffic inning avoidance
    ops_tto: Optional[float] = None     # times-through-order penalty awareness
    ops_bpen: Optional[float] = None    # bullpen quality behind pitcher
    ops_inj: Optional[float] = None     # injury risk score (inverse)
    ops_trend: Optional[float] = None   # recent performance trend
    ops_fat: Optional[float] = None     # fatigue (days rest)

    # UHS — Umpire Hits Score
    uhs_cstr: Optional[float] = None    # called-strike rate
    uhs_zone: Optional[float] = None    # zone accuracy
    uhs_early: Optional[float] = None   # early-count strike tendency
    uhs_weak: Optional[float] = None    # weak contact encouragement

    # DSC — Defense Score
    dsc_def: Optional[float] = None     # overall defensive rating
    dsc_infdef: Optional[float] = None  # infield defense
    dsc_ofdef: Optional[float] = None   # outfield defense
    dsc_catch: Optional[float] = None   # catcher framing/defense
    dsc_align: Optional[float] = None   # defensive alignment score

    # ────────────────────────────────────────────────────────────────────
    # KUSI feature scores (0-100 each, 50 = neutral)
    # ────────────────────────────────────────────────────────────────────

    # OCR — Opponent Contact Rate
    ocr_k: Optional[float] = None       # opponent strikeout rate
    ocr_con: Optional[float] = None     # opponent contact rate (inverse)
    ocr_zcon: Optional[float] = None    # opponent zone-contact rate (inverse)
    ocr_disc: Optional[float] = None    # opponent plate discipline (low = good for K)
    ocr_2s: Optional[float] = None      # opponent two-strike performance
    ocr_foul: Optional[float] = None    # opponent foul-ball tendency
    ocr_dec: Optional[float] = None     # opponent decision making (swing%)

    # PMR — Pitch Mix Rating
    pmr_p1: Optional[float] = None      # primary pitch dominance
    pmr_p2: Optional[float] = None      # secondary pitch complement
    pmr_put: Optional[float] = None     # putaway pitch effectiveness
    pmr_run: Optional[float] = None     # running game impact (disrupts K counts)
    pmr_top6: Optional[float] = None    # top-6 K batters in lineup
    pmr_plat: Optional[float] = None    # platoon advantage score

    # PER — Pitcher Efficiency Rating
    per_ppa: Optional[float] = None     # pitches per at-bat (efficiency)
    per_bb: Optional[float] = None      # walk rate (inverse)
    per_fps: Optional[float] = None     # first-pitch strike rate
    per_deep: Optional[float] = None    # ability to pitch deep into games
    per_putw: Optional[float] = None    # putaway rate on two-strike counts
    per_cmdd: Optional[float] = None    # command/location score
    per_velo: Optional[float] = None    # velocity score

    # KOP — K-Operational Profile
    kop_pcap: Optional[float] = None    # pitch count capacity for Ks
    kop_hook: Optional[float] = None    # hook tendency (fewer Ks if quick hook)
    kop_tto: Optional[float] = None     # times-through-order K awareness
    kop_bpen: Optional[float] = None    # bullpen preserves starter K opportunity
    kop_pat: Optional[float] = None     # patience/approach vs this pitcher
    kop_inj: Optional[float] = None     # injury risk (inverse)
    kop_fat: Optional[float] = None     # fatigue

    # UKS — Umpire K Score
    uks_tight: Optional[float] = None   # tight zone (fewer calls → fewer Ks)
    uks_cstrl: Optional[float] = None   # called-strike rate
    uks_2exp: Optional[float] = None    # two-strike expansion tendency
    uks_count: Optional[float] = None   # count manipulation score

    # TLR — Top-Lineup Resistance
    tlr_top4k: Optional[float] = None   # top-4 K batters
    tlr_top6c: Optional[float] = None   # top-6 contact batters
    tlr_vet: Optional[float] = None     # veteran presence (disciplined AB)
    tlr_top2: Optional[float] = None    # top-2 hitters in lineup difficulty

    # ── Flags for interaction/volatility rule logic
    fly_ball_suppression: Optional[float] = None  # used in H2
    pitcher_median_ks: Optional[float] = None     # used in K5 interaction
    relies_on_one_putaway: bool = False            # used in K2 interaction

    # ── Season ERA Tier — direct HUSI penalty for struggling pitchers
    # Source: season ERA from MLB Stats API (filled by feature_builder)
    # HV10 triggers when ERA ≥ 5.00 — a pitcher giving up runs all season is a hit risk
    season_era_raw: Optional[float] = None    # actual season ERA (e.g. 5.95)
    season_era_tier: str = "NORMAL"           # NORMAL / STRUGGLING (5.00-5.99) / DISASTER (6.00+)
    lineup_discipline_score: Optional[float] = None  # used in K3 interaction
    weak_edge_command: bool = False                   # used in K7 interaction
    babip_variance_high: bool = False              # used in HV3
    recent_velocity_spike: bool = False            # used in KV4
    key_contact_bats_uncertain: bool = False       # used in KV5
    opponent_boom_bust_volatility: bool = False    # used in KV8

    # ── Bullpen Fatigue Coefficient (β_bp)
    # BFS (Bullpen Fatigue Score) = (avg_weighted_pitches_per_leverage_arm - 15) / 100
    # Clamped to [-0.20, 0.50]. 0.0 = neutral/fresh.
    bullpen_fatigue_opp: float = 0.0      # opponent's bullpen fatigue → feeds HUSI adjustment
    bullpen_fatigue_own: float = 0.0      # own team's bullpen fatigue → feeds KUSI adjustment
    bullpen_red_alert_opp: bool = False   # opponent closer threw back-to-back days
    bullpen_red_alert_own: bool = False   # own closer threw back-to-back days
    bullpen_label_opp: str = "NO DATA"   # human-readable: FRESH/NORMAL/TIRED/GASSED/RED ALERT
    bullpen_label_own: str = "NO DATA"   # human-readable: FRESH/NORMAL/TIRED/GASSED/RED ALERT

    # ── Mid-Game Surge (MGS) state
    # Tracks how far into the game the pitcher is so the TTO acceleration
    # formula can shift the projection curve in real time.
    # In pre-game mode all three default to 0 and the formula uses expected IP instead.
    mgs_inning: int = 0           # current inning (0 = pre-game)
    mgs_pitch_count: int = 0      # pitcher's current pitch count (0 = pre-game)
    mgs_tto: int = 0              # times-through-order tier reached (0 = pre-game)

    # ── Pitcher Form Factor (PFF)
    # Captures how HOT or COLD this pitcher is entering today's game.
    # Computed from last 3 starts — weighted toward recency.
    # Positive PFF = above his own baseline (HOT).
    # Negative PFF = below his own baseline (COLD / STRUGGLING).
    # PFF modifies TTO1 multipliers in the MGS formula:
    #   HOT  → more suppression early, then steeper surge in TTO2/TTO3
    #   COLD → struggles from inning 1, normal TTO2/TTO3 plateau
    pff_score: float = 0.0               # [-0.30, +0.30] delta vs pitcher's season avg
    pff_label: str = "NEUTRAL"           # ON FIRE / HOT / NEUTRAL / COLD / STRUGGLING
    pff_hits_tto1_mult: float = 0.82     # adjusted TTO1 hits multiplier
    pff_ks_tto1_mult: float = 1.18       # adjusted TTO1 Ks multiplier
    pff_tto_late_boost: float = 0.0      # additional % on TTO2/TTO3 hits (for HOT pitchers)
    pff_starts_used: int = 0             # how many recent starts contributed

    # ── SKU #37 — Catcher Framing Module
    # The defending catcher can "steal" borderline strikes through elite framing,
    # effectively giving the pitcher free Ks the formula would not otherwise count.
    # Strike rate > 50% triggers a +4% KUSI multiplier.
    # Strike rate < 48% triggers a -2% KUSI penalty.
    catcher_id: Optional[str] = None           # MLB player ID of the catcher
    catcher_name: Optional[str] = None         # catcher display name
    catcher_framing_score: float = 50.0        # 0-100 framing score (feeds dsc_catch block)
    catcher_strike_rate: float = 50.0          # raw Statcast called-strike rate (%)
    catcher_kusi_adj: float = 0.0              # signed KUSI multiplier (+0.04 / 0.0 / -0.02)
    catcher_framing_label: str = "NEUTRAL"     # ELITE / ABOVE_AVG / AVG / BELOW_AVG / POOR

    # ── SKU #14 — Travel & Fatigue Index (TFI)
    # Measures rest deficit and timezone disruption for the PITCHING TEAM.
    # If rest < 16 hours (getaway day) OR timezone shift >= 2 hours,
    # a -7% Reaction Penalty is applied to HUSI (#27).
    tfi_rest_hours: float = 24.0               # hours between last game's end and today's first pitch
    tfi_tz_shift: int = 0                      # absolute timezone delta from yesterday's venue
    tfi_getaway_day: bool = False              # True when rest_hours < 16
    tfi_cross_timezone: bool = False           # True when tz_shift >= 2
    tfi_penalty_pct: float = 0.0              # 0.07 when triggered, else 0.0
    tfi_label: str = "NO DATA"                 # RESTED / GETAWAY DAY / CROSS_TZ / GETAWAY+CROSS_TZ

    # ── SKU #38 — VAA & Extension "Perceived Velocity"
    # Vertical Approach Angle (VAA): how steeply the ball descends at the plate.
    # Extension: how far in front of the rubber the pitcher releases (feet).
    # Both are calculated from the MLB Stats API live game feed.
    #
    # Extension boost: > 6.8 ft → +1.5 mph perceived velocity → boosts per_velo in KUSI
    # VAA flat penalty: < -4.5° → ball is easier to track → +10% contact probability in HUSI
    #
    # Convention: VAA is a negative angle (ball descends). Flatter = less negative (closer to 0°).
    # The user spec defines "flat" as < -4.5° (i.e. more negative than -4.5°).
    vaa_degrees: Optional[float] = None        # average VAA this game (negative degrees)
    extension_ft: Optional[float] = None       # average release extension (feet)
    vaa_flat: bool = False                     # True when vaa_degrees < -4.5°
    extension_elite: bool = False              # True when extension_ft > 6.8 ft
    vaa_contact_penalty: float = 0.0          # 0.10 when vaa_flat is True, else 0.0
    extension_velo_boost: float = 0.0         # per_velo score boost when extension_elite
