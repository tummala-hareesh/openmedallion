"""cortex/theme.py — Design tokens for the Data Studio Pro theme."""
from __future__ import annotations

# ── Accent ────────────────────────────────────────────────────────────────────
TEAL        = "#10B981"
TEAL_DARK   = "#059669"
TEAL_BG     = "#ECFDF5"
TEAL_BORDER = "#A7F3D0"
TEAL_ACCENT = "rgba(16,185,129,0.08)"

# ── Structure ─────────────────────────────────────────────────────────────────
SIDEBAR_BG   = "#1E293B"
SIDEBAR_LINE = "rgba(255,255,255,0.07)"
MAIN_BG      = "#F8FAFC"
WHITE        = "#FFFFFF"
BORDER       = "#E2E8F0"

# ── Text ──────────────────────────────────────────────────────────────────────
TEXT_PRIMARY   = "#0F172A"
TEXT_SECONDARY = "#1E293B"
TEXT_DIM       = "#475569"
TEXT_MUTED     = "#94A3B8"   # ≥4.7:1 on SIDEBAR_BG — passes WCAG AA

# ── Semantic ──────────────────────────────────────────────────────────────────
GREEN_DARK = "#047857"   # 4.6:1 on white — WCAG AA for small text
RED_DARK   = "#B91C1C"   # 5.9:1 on white — WCAG AA for small text
AMBER      = "#D97706"

# ── Code blocks ───────────────────────────────────────────────────────────────
SQL_BG     = "#0F172A"
SQL_BORDER = "#334155"
SQL_TEXT   = "#94A3B8"
SQL_KW     = TEAL
SQL_COL    = "#7DD3FC"
SQL_STR    = "#FCA5A5"

# ── Typography ────────────────────────────────────────────────────────────────
FONT_UI   = "'DM Sans', system-ui, sans-serif"
FONT_MONO = "'JetBrains Mono', 'Fira Code', monospace"

# ── Chart palette (teal → green → slate) ─────────────────────────────────────
CHART_PALETTE = ["#10B981", "#059669", "#34D399", "#6EE7B7", "#475569", "#94A3B8"]

# ── Nav button styles (returned by callbacks) ─────────────────────────────────
_NAV_BASE: dict = {
    "display": "flex",
    "alignItems": "center",
    "width": "100%",
    "padding": "10px 20px",
    "background": "transparent",
    "border": "none",
    "borderLeft": "3px solid transparent",
    "fontSize": "13.5px",
    "fontWeight": "500",
    "cursor": "pointer",
    "textAlign": "left",
    "fontFamily": FONT_UI,
    "transition": "color 0.15s",
    "outline": "none",
}


def nav_style(active: bool) -> dict:
    if active:
        return {**_NAV_BASE, "color": WHITE, "borderLeftColor": TEAL, "background": TEAL_ACCENT}
    return {**_NAV_BASE, "color": TEXT_MUTED}


# ── Content panel show/hide ───────────────────────────────────────────────────
def panel_style(visible: bool) -> dict:
    return {"display": "block", "height": "100%"} if visible else {"display": "none"}
