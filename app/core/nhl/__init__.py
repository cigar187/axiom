"""
NHL scoring engines — GSAUI and PPUI.

GSAUI: Goalie Shots-Against Under Index (mirrors HUSI block architecture)
PPUI:  Player Points Under Index       (mirrors KUSI block architecture)

These engines are completely standalone from the baseball pipeline.
They read from NHLGoalieFeatureSet / NHLSkaterFeatureSet and produce
a 0-100 index plus a projected counting stat.
"""
