"""
NHL scoring engines — GSAI and PPSI.

GSAI: Goalie Shots-Against Index (mirrors HUSI block architecture)
PPSI:  Player Points Scoring Index       (mirrors KUSI block architecture)

These engines are completely standalone from the baseball pipeline.
They read from NHLGoalieFeatureSet / NHLSkaterFeatureSet and produce
a 0-100 index plus a projected counting stat.
"""
