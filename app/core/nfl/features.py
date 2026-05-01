"""
QBFeatureSet — the single data contract between data fetchers and the NFL scoring engines.

The scoring engine only reads from this dataclass.
Data fetchers only write to this dataclass.
This hard boundary means you can swap any data source without touching the scoring logic.

Convention (mirrors PitcherFeatureSet):
  - All block sub-scores are already normalized to 0–100 before entering here
  - 50  = neutral (no edge either way)
  - >50 = favors QB output  (more yards / more TDs)
  - <50 = suppresses QB output
  - None fields fall back to NEUTRAL (50) inside the engines

Architecture:
  QPYI_base = 0.23×OSW + 0.20×QSR + 0.14×GSP + 0.12×SCB + 0.10×PDR + 0.09×ENS + 0.07×DSR + 0.05×RCT
  QTDI_base = 0.24×ORD + 0.20×QTR + 0.15×GSP_TD + 0.12×SCB_TD + 0.10×PDR + 0.07×DSR + 0.07×ENS + 0.05×RCT

Blocks (8 blocks, 58 total inputs — QPYI fully received):
  OSW  Opponent Secondary Weakness      9 inputs   (RECEIVED)
  QSR  QB Skill Rating                 10 inputs   (RECEIVED)
  GSP  Game Script Profile              7 inputs   (RECEIVED)
  SCB  Supporting Cast Block            6 inputs   (RECEIVED)
  PDR  Physical Durability Rating      10 inputs   (RECEIVED)
  ENS  Environmental                    7 inputs   (RECEIVED)
  DSR  Defensive Scheme Rating          5 inputs   (RECEIVED)
  RCT  Referee Crew Tendencies          4 inputs   (RECEIVED)

Sub-block weights for all 8 blocks: PENDING — awaiting from user
QTDI formula: PENDING — awaiting from user
Interaction boosts: PENDING — awaiting from user
Volatility penalties: PENDING — awaiting from user
Projection formula: PENDING — awaiting from user
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class QBFeatureSet:

    # ── Identity ─────────────────────────────────────────────────────────────
    player_id:     int  = 0
    player_name:   str  = ""
    team:          str  = ""       # team abbreviation, e.g. "KC"
    opponent:      str  = ""       # opponent abbreviation, e.g. "CHI"
    game_id:       str  = ""
    game_date:     str  = ""       # YYYY-MM-DD
    is_home:       bool = True
    nfl_seasons:   int  = 1        # seasons of NFL experience — used for age/wear curve in PDR

    # ── Prop lines (filled by odds fetcher)
    pass_yards_line: Optional[float] = None
    pass_yards_over_odds:  Optional[float] = None
    pass_yards_under_odds: Optional[float] = None
    td_line:        Optional[float] = None
    td_over_odds:   Optional[float] = None
    td_under_odds:  Optional[float] = None

    # ── Base rates (Bayesian-shrunk toward league averages)
    # League baselines: ~230 pass yds/game, ~1.8 TDs/game.
    # Small-sample season rates are pulled toward league average — same
    # Bayesian shrinkage logic as blended_h_per_9 / blended_k_per_9 in baseball.
    blended_yards_per_game: float = 230.0
    blended_tds_per_game:   float = 1.8
    season_games_started:   int   = 0     # starts this season — controls shrinkage weight

    # ── Post-formula multipliers (applied after QPYI/QTDI score is computed)
    # Mirrors park_hits_multiplier / fi_hits_mult pattern from baseball.
    # park_turf_mult: stadium-specific passing yards modifier.
    #   e.g. 1.08 for a dome on fast artificial turf, 0.93 for cold outdoor grass.
    #   PENDING — exact derivation formula awaiting piece from user.
    # pdr_rest_mult: rest-window modifier derived from pdr_rest days.
    #   e.g. 0.93 for Thursday night (4 days rest), 1.05 for bye week.
    #   PENDING — exact derivation formula awaiting piece from user.
    park_turf_mult: float = 1.0   # applied to projected_yards only
    pdr_rest_mult:  float = 1.0   # applied to both projected_yards and projected_tds

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 1 — OSW: Opponent Secondary Weakness  (QPYI 23%)
    # How exploitable is the opposing defense this week?
    # High score = weak secondary = favors QB production.
    #
    # Sources:
    #   osw_cb, osw_slot          → Pro Football Focus (PFF)
    #   osw_safety                → Next Gen Stats / Mike Lopez
    #   osw_yat                   → NFL Stats API
    #   osw_cmp                   → nflfastR / Ben Baldwin
    #   osw_air                   → Josh Hermsmeyer (FiveThirtyEight — Air Yards)
    #   osw_blitz                 → Next Gen Stats / Mike Lopez
    #   osw_press                 → Next Gen Stats / Quang Nguyen (STRAIN metric)
    #   osw_dvoa                  → Aaron Schatz (Football Outsiders — DVOA)
    # ─────────────────────────────────────────────────────────────────────────
    osw_cb:     Optional[float] = None  # CB coverage grade — inverted: lower PFF grade = higher score
    osw_slot:   Optional[float] = None  # slot CB coverage grade — 30-40% of pass volume routes here
    osw_safety: Optional[float] = None  # safety help rate: 2-high vs single-high shell (disguises weak CBs)
    osw_yat:    Optional[float] = None  # yards allowed per target (high = weak secondary = high score)
    osw_cmp:    Optional[float] = None  # completion % allowed by secondary (high = weak)
    osw_air:    Optional[float] = None  # air yards surrendered per game
    osw_blitz:  Optional[float] = None  # blitz rate — inverted: high blitz hurts QB (inverted score)
    osw_press:  Optional[float] = None  # pressure rate generated — inverted
    osw_dvoa:   Optional[float] = None  # opponent pass defense DVOA (high = weaker defense = high score)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 2 — QSR: QB Skill Rating  (QPYI 20%)
    # What can this quarterback do on his own?
    # High score = elite individual QB skill level this week.
    #
    # Sources:
    #   qsr_cpoe, qsr_pa_rate, qsr_pa_cpoe → nflfastR / Ben Baldwin (The Athletic)
    #   qsr_air                             → Josh Hermsmeyer (FiveThirtyEight — Air Yards)
    #   qsr_pres_cmp, qsr_ttt               → Next Gen Stats / Mike Lopez
    #   qsr_deep, qsr_offplat               → Pro Football Focus
    #   qsr_mech                            → Glenn Fleisig PhD (ASMI) / Gregory Rash
    #   qsr_presnap                         → Next Gen Stats / Cynthia Frelund (NFL Network)
    # ─────────────────────────────────────────────────────────────────────────
    qsr_cpoe:     Optional[float] = None  # completion % over expected given throw difficulty
    qsr_air:      Optional[float] = None  # air yards per attempt — how far downfield he throws
    qsr_pres_cmp: Optional[float] = None  # completion % when under pressure (hit or hurried)
    qsr_ttt:      Optional[float] = None  # time to throw — inverted: faster release = higher score
    qsr_deep:     Optional[float] = None  # deep ball accuracy 20+ yards (PFF)
    qsr_offplat:  Optional[float] = None  # off-platform / movement throw accuracy (PFF)
    qsr_mech:     Optional[float] = None  # kinematic sequencing / shoulder-elbow torque efficiency
    qsr_presnap:  Optional[float] = None  # pre-snap processing: audible rate, motion usage, shifts
    qsr_pa_rate:  Optional[float] = None  # play action usage rate — creates major efficiency gains
    qsr_pa_cpoe:  Optional[float] = None  # completion accuracy above expected on play action snaps

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 3 — GSP: Game Script Profile  (QPYI 14%)
    # How many opportunities will this QB actually get?
    # High score = game script produces many passing opportunities.
    #
    # Sources:
    #   gsp_pcall, gsp_oc_trend → Warren Sharp (Sharp Football Analysis)
    #   gsp_spread, gsp_total   → Vegas lines
    #   gsp_snaps               → nflfastR / Brian Burke
    #   gsp_pace                → NFL Stats
    #   gsp_rz                  → NFL Stats / Dean Oliver (ESPN QBR)
    # ─────────────────────────────────────────────────────────────────────────
    gsp_pcall:    Optional[float] = None  # play caller pass rate tendency vs. league avg
    gsp_spread:   Optional[float] = None  # Vegas spread — underdog role = trailing = more passes
    gsp_total:    Optional[float] = None  # Vegas game O/U — high total = high volume environment
    gsp_snaps:    Optional[float] = None  # projected QB snap count
    gsp_pace:     Optional[float] = None  # offensive pace (plays per game)
    gsp_rz:       Optional[float] = None  # red zone trip rate — frequency of scoring opportunities
    gsp_oc_trend: Optional[float] = None  # OC scheme novelty / hot streak — short-term edge

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 4 — SCB: Supporting Cast Block  (QPYI 12%)
    # Who is around him helping him succeed?
    # High score = strong supporting cast this week.
    #
    # Sources:
    #   scb_pblk          → ESPN Next Gen Stats / Thompson Bliss (NFL Physics)
    #   scb_sep           → Next Gen Stats / Mike Lopez
    #   scb_yac           → nflfastR / Brian Burke (EPA)
    #   scb_te            → NFL Stats
    #   scb_inj           → Official NFL injury report
    #   scb_ryoe          → Next Gen Stats / Brian Burke (EPA splits)
    # ─────────────────────────────────────────────────────────────────────────
    scb_pblk: Optional[float] = None  # O-line pass block win rate — how often linemen win before pressure
    scb_sep:  Optional[float] = None  # receiver separation rate — how open targets get off coverage
    scb_yac:  Optional[float] = None  # yards after catch quality — receiver production after the catch
    scb_te:   Optional[float] = None  # tight end target quality — reliability as safety valve and RZ threat
    scb_inj:  Optional[float] = None  # receiver injury status (0=key WR out, 100=fully healthy)
    scb_ryoe: Optional[float] = None  # run game quality: rushing yards over expectation — keeps D honest

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 5 — PDR: Physical Durability Rating  (QPYI 10%)
    # How is his body holding up?
    # Unique to NFL — no equivalent block exists in the MLB formula.
    # High score = QB is physically fresh, healthy, and mobile this week.
    #
    # Sources:
    #   pdr_sack            → NFL Stats / Brad Oremland (QB-TSP)
    #   pdr_press           → Next Gen Stats / Rishav Dutta (Cleveland Browns)
    #   pdr_mob             → Next Gen Stats
    #   pdr_hits            → NFL Injury Surveillance System / Kelly et al. (EMG study)
    #   pdr_rest            → NFL schedule
    #   pdr_snaps_prior     → nflfastR play-by-play
    #   pdr_prac            → Official NFL injury report
    #   pdr_inj             → Official NFL injury report
    #   pdr_age             → John DeWitt PhD (NASA / Rice University)
    #   pdr_trend           → Andrew Patton PhD (Johns Hopkins / NFL Analytics)
    # ─────────────────────────────────────────────────────────────────────────
    pdr_sack:        Optional[float] = None  # sack rate per dropback — inverted: high rate = low score
    pdr_press:       Optional[float] = None  # pressure rate absorbed per game — inverted
    pdr_mob:         Optional[float] = None  # pocket mobility: escape rate under pressure
    pdr_hits:        Optional[float] = None  # hits taken per game including post-throw contact — inverted
    pdr_rest:        Optional[float] = None  # rest days: 4=Thursday (severe penalty), 7=normal, 14=bye (boost)
    pdr_snaps_prior: Optional[float] = None  # prior week snap count — high snap TNF game compounds fatigue
    pdr_prac:        Optional[float] = None  # practice participation: 0=DNP, 50=limited, 100=full
    pdr_inj:         Optional[float] = None  # injury designation: 0=out, 33=doubtful, 66=questionable, 100=clear
    pdr_age:         Optional[float] = None  # age + career hits wear curve (John DeWitt PhD / NASA methodology)
    pdr_trend:       Optional[float] = None  # rolling game-to-game degradation signal (Andrew Patton PhD)

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 6 — ENS: Environmental  (QPYI 9%)
    # What are the conditions on the field?
    # High score = conditions favor passing production.
    #
    # Sources:
    #   ens_dome            → NFL schedule data
    #   ens_wind, ens_temp, ens_precip → Weather API / Thompson Bliss (NFL Physics)
    #   ens_turf, ens_alt   → Stadium data
    #   ens_crowd           → PFF road/home splits / stadium noise ratings
    # ─────────────────────────────────────────────────────────────────────────
    ens_dome:   Optional[float] = None  # dome indicator (100=dome/retractable closed, 0=fully outdoor)
    ens_wind:   Optional[float] = None  # wind speed — inverted: >15 mph meaningfully hurts passing
    ens_temp:   Optional[float] = None  # temperature — inverted: <32°F hurts grip, routes, ball travel
    ens_precip: Optional[float] = None  # rain/snow probability — inverted: wet ball hurts accuracy
    ens_turf:   Optional[float] = None  # surface type (100=artificial turf, 0=natural grass)
    ens_alt:    Optional[float] = None  # altitude — affects ball flight distance and receiver breathing
    ens_crowd:  Optional[float] = None  # crowd noise / road environment — inverted: loud road venues hurt QB communication

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 7 — DSR: Defensive Scheme Rating  (QPYI 7%)
    # How well does THIS QB read and attack THIS specific scheme?
    # NEW BLOCK — not present in MLB formula.
    #
    # Captures the most predictive gap in the original formula: the specific
    # matchup between a QB's tendencies and this week's DC scheme.
    # Josh Allen vs. Tampa-2 is a fundamentally different problem than
    # Josh Allen vs. single-high man — previously buried inside OSW / QSR averages.
    #
    # Sources:
    #   dsr_zone_eff, dsr_matchup_hist → nflfastR / Cynthia Frelund (NFL Network)
    #   dsr_man_eff                    → nflfastR / Ben Baldwin
    #   dsr_blitz_eff                  → Next Gen Stats / Mike Lopez
    #   dsr_dc_scheme                  → Ted Nguyen (The 33rd Team)
    # ─────────────────────────────────────────────────────────────────────────
    dsr_zone_eff:     Optional[float] = None  # this QB's EPA + CPOE historically against zone coverage
    dsr_man_eff:      Optional[float] = None  # this QB's EPA + CPOE historically against man coverage
    dsr_blitz_eff:    Optional[float] = None  # this QB's performance when blitzed — some feast, some collapse
    dsr_dc_scheme:    Optional[float] = None  # DC base scheme identity score (4-3, 3-4, Cover-2, Tampa-2, Cover-3, man-heavy)
    dsr_matchup_hist: Optional[float] = None  # head-to-head QB performance vs. this specific DC regardless of team

    # ─────────────────────────────────────────────────────────────────────────
    # BLOCK 8 — RCT: Referee Crew Tendencies  (QPYI 5%)
    # How will the officials affect this game?
    # High score = this crew's tendencies favor passing volume and opportunities.
    #
    # Sources:
    #   rct_pi, rct_rtp → Warren Sharp (Sharp Football Analysis)
    #   rct_hold, rct_total → NFL Stats
    # ─────────────────────────────────────────────────────────────────────────
    rct_pi:    Optional[float] = None  # pass interference flags per game — extends drives and passing volume
    rct_rtp:   Optional[float] = None  # roughing the passer calls per game — extends drives and pass attempts
    rct_hold:  Optional[float] = None  # defensive holding rate per game — extends drives and total attempts
    rct_total: Optional[float] = None  # total flags per game — high-flag crews create more passing opportunities

    # =========================================================================
    # QTDI-SPECIFIC BLOCKS
    # QTDI_base = 0.24×ORD + 0.20×QTR + 0.15×GSP_TD + 0.12×SCB_TD
    #           + 0.10×PDR + 0.07×DSR + 0.07×ENS + 0.05×RCT
    # PDR / DSR / ENS / RCT are shared with QPYI — same fields, different weights.
    # =========================================================================

    # ─────────────────────────────────────────────────────────────────────────
    # ORD: Opponent Red Zone Defense  (QTDI 24%)
    # Replaces OSW. How well does this defense stop TDs inside the 20?
    # High score = leaky red zone defense = favors QB TD production.
    #
    # Sources:
    #   ord_rz_dvoa → Aaron Schatz (Football Outsiders — DVOA red zone splits)
    #   All others  → NFL Stats / Dean Oliver (ESPN QBR)
    # ─────────────────────────────────────────────────────────────────────────
    ord_rz_yards_allowed:    Optional[float] = None  # red zone yards allowed per game
    ord_td_rate:             Optional[float] = None  # TD rate allowed in the red zone (high = weak = high score)
    ord_goal_line_stop_rate: Optional[float] = None  # goal line stand rate — inverted: elite stand = low score
    ord_short_yardage_rank:  Optional[float] = None  # short-yardage defensive ranking — inverted
    ord_rz_dvoa:             Optional[float] = None  # red zone DVOA split (Aaron Schatz — high = weaker RZ defense)

    # ─────────────────────────────────────────────────────────────────────────
    # QTR: QB Touchdown Rate Block  (QTDI 20%)
    # Replaces QSR. TD-specific efficiency rather than overall passing accuracy.
    # High score = this QB converts opportunities into touchdowns at a high rate.
    #
    # Sources:
    #   qtr_pa_td_rate → nflfastR / Ben Baldwin
    #   All others     → NFL Stats / Dean Oliver (ESPN QBR)
    # ─────────────────────────────────────────────────────────────────────────
    qtr_td_rate_per_rz_trip: Optional[float] = None  # TD rate per red zone trip
    qtr_pa_td_rate:          Optional[float] = None  # play action TD rate (nflfastR / Ben Baldwin)
    qtr_sneak_tendency:      Optional[float] = None  # QB sneak tendency and success rate at the goal line
    qtr_q4_clutch_td_rate:   Optional[float] = None  # fourth quarter clutch TD rate
    qtr_third_down_conv_rate: Optional[float] = None  # third down conversion rate
    qtr_goal_line_carry_rate: Optional[float] = None  # goal line carry rate — dual-threat QB advantage at the 1

    # ─────────────────────────────────────────────────────────────────────────
    # GSP_TD: Game Script Profile — TD version  (QTDI 15%)
    # PENDING — awaiting detail piece from user
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # SCB_TD: Supporting Cast Block — TD version  (QTDI 12%)
    # PENDING — awaiting detail piece from user
    # ─────────────────────────────────────────────────────────────────────────
