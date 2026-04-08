"""
app/carbon.py

IBM Carbon Design System — Gray 100 (dark) theme tokens and HTML helpers.
Reference: DESIGN.md in project root.

Usage in any page:
    from app.carbon import inject, hero, kpi_card, badge, divider
    inject()   # call once at top of each page

All HTML components follow Carbon rules:
- 0px border-radius on cards and banners (exception: badges = 24px pill)
- No gradients, no shadows on cards
- IBM Plex Sans (display weight 300, body 400, label 600)
- IBM Plex Mono for numeric values
- Single accent: IBM Blue 60 (#0f62fe) / Blue 40 (#78a9ff) on dark bg
- 8px base grid spacing
"""
import streamlit as st

# ── Dark theme tokens ──────────────────────────────────────────────────────────
BG           = "#161616"   # Gray 100 — page background
LAYER_01     = "#262626"   # Gray 90 — card / container surface
LAYER_02     = "#393939"   # Gray 80 — elevated surface within card
BORDER       = "#393939"   # Gray 80 — subtle border
TEXT_PRIMARY  = "#f4f4f4"  # Gray 10 — primary text
TEXT_SECONDARY = "#c6c6c6" # Gray 30 — secondary / label text
TEXT_DISABLED  = "#6f6f6f" # Gray 60 — disabled / placeholder

# Accent
BLUE_40      = "#78a9ff"   # interactive on dark bg (links, focus)
BLUE_60      = "#0f62fe"   # primary CTA, button fill

# Status (Carbon support colors adapted for hydrology)
C_CRITICAL   = "#da1e28"   # Red 60 — flood warning
C_WATCH      = "#f1c21b"   # Yellow 30 — flood watch
C_NORMAL     = "#24a148"   # Green 50 — normal flow
C_LOW_FLOW   = "#78a9ff"   # Blue 40 — low flow (informational)
C_NODATA     = "#6f6f6f"   # Gray 60 — no data

# Fonts
FONT_SANS = "'IBM Plex Sans','Helvetica Neue',Arial,sans-serif"
FONT_MONO = "'IBM Plex Mono',Menlo,Courier,monospace"

# ── Global font + CSS injection ────────────────────────────────────────────────
_FONTS_INJECTED = False

def inject() -> None:
    """
    Inject IBM Plex fonts and global Carbon CSS overrides.
    Call once at the top of every page.
    """
    global _FONTS_INJECTED
    st.markdown(f"""
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;600&family=IBM+Plex+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  /* ── Global Carbon overrides ── */
  html, body, [class*="css"], .stApp {{
    font-family: {FONT_SANS};
    background-color: {BG};
    color: {TEXT_PRIMARY};
  }}
  /* Streamlit headings */
  h1, h2, h3, h4 {{
    font-family: {FONT_SANS};
    font-weight: 300;
    color: {TEXT_PRIMARY};
  }}
  /* Streamlit metric labels */
  [data-testid="stMetricLabel"] {{
    font-size: 12px;
    font-weight: 400;
    letter-spacing: 0.32px;
    color: {TEXT_SECONDARY};
    text-transform: uppercase;
    font-family: {FONT_SANS};
  }}
  [data-testid="stMetricValue"] {{
    font-family: {FONT_MONO};
    font-size: 28px;
    font-weight: 400;
    color: {TEXT_PRIMARY};
  }}
  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {{
    background: {LAYER_01};
    border-radius: 0;
    gap: 0;
  }}
  .stTabs [data-baseweb="tab"] {{
    font-family: {FONT_SANS};
    font-size: 14px;
    font-weight: 400;
    letter-spacing: 0.16px;
    color: {TEXT_SECONDARY};
    border-radius: 0;
    padding: 12px 16px;
    border-bottom: 2px solid transparent;
  }}
  .stTabs [aria-selected="true"] {{
    color: {TEXT_PRIMARY};
    border-bottom: 2px solid {BLUE_40};
    background: {LAYER_02};
  }}
  /* Selectbox / dropdown */
  .stSelectbox > div > div {{
    background: {LAYER_01};
    border: none;
    border-bottom: 2px solid {BORDER};
    border-radius: 0;
    color: {TEXT_PRIMARY};
    font-family: {FONT_SANS};
  }}
  /* Dataframe */
  .stDataFrame {{
    font-family: {FONT_SANS};
    font-size: 14px;
  }}
  /* Sidebar */
  [data-testid="stSidebar"] {{
    background: {LAYER_01};
  }}
  /* Divider */
  hr {{
    border-color: {BORDER};
  }}
  /* Info/warning boxes */
  .stAlert {{
    border-radius: 0;
    border-left: 3px solid {BLUE_40};
  }}
</style>
""", unsafe_allow_html=True)
    _FONTS_INJECTED = True


# ── Component builders ─────────────────────────────────────────────────────────

def hero(
    title: str,
    subtitle: str = "",
    right_label: str = "",
    right_value: str = "",
    right_label2: str = "",
    right_value2: str = "",
    status_text: str = "",
    status_color: str = C_NORMAL,
) -> str:
    """
    Full-width Carbon hero banner.
    Flat #161616 background, IBM Plex Sans weight 300 heading.
    No gradients. Optional right-side KPIs and status badge.
    """
    right_html = ""
    if right_label and right_value:
        right2 = f"""
        <div>
          <div style="color:{TEXT_SECONDARY};font-size:11px;font-weight:400;
                      letter-spacing:0.32px;text-transform:uppercase">{right_label2}</div>
          <div style="color:{TEXT_PRIMARY};font-size:28px;font-weight:300;
                      font-family:{FONT_MONO};line-height:1.17">{right_value2}</div>
        </div>""" if right_label2 else ""

        right_html = f"""
      <div style="display:flex;gap:40px;align-items:center">
        <div>
          <div style="color:{TEXT_SECONDARY};font-size:11px;font-weight:400;
                      letter-spacing:0.32px;text-transform:uppercase">{right_label}</div>
          <div style="color:{TEXT_PRIMARY};font-size:28px;font-weight:300;
                      font-family:{FONT_MONO};line-height:1.17">{right_value}</div>
        </div>
        {right2}
        {badge(status_text, status_color) if status_text else ""}
      </div>"""

    return f"""
<div style="background:{BG};border-bottom:1px solid {BORDER};
            padding:32px 32px 24px;margin:-1rem -1rem 1.5rem;
            display:flex;align-items:flex-end;justify-content:space-between">
  <div>
    <h1 style="font-family:{FONT_SANS};font-size:42px;font-weight:300;
               line-height:1.19;color:{TEXT_PRIMARY};margin:0;padding:0">{title}</h1>
    <p style="font-family:{FONT_SANS};font-size:14px;font-weight:400;
              letter-spacing:0.16px;color:{TEXT_SECONDARY};margin:8px 0 0">{subtitle}</p>
  </div>
  {right_html}
</div>"""


def kpi_card(
    label: str,
    value: str,
    sub: str = "",
    trend: str = "",
    color: str = BLUE_40,
    wide: bool = False,
) -> str:
    """
    Carbon tile (KPI card).
    Layer 01 background (#262626), 0px border-radius, 16px padding.
    Left border-bottom accent line in status color.
    Value in IBM Plex Mono.
    """
    w = "min-width:160px;" if wide else ""
    sub_html = f'<div style="color:{TEXT_DISABLED};font-size:12px;font-weight:400;letter-spacing:0.32px;margin-top:4px">{sub}</div>' if sub else ""
    trend_html = f'<span style="color:{color};font-size:16px;margin-left:6px">{trend}</span>' if trend else ""
    return f"""
<div style="background:{LAYER_01};border-bottom:3px solid {color};
            padding:16px;{w}font-family:{FONT_SANS};border-radius:0">
  <div style="color:{TEXT_SECONDARY};font-size:11px;font-weight:400;
              letter-spacing:0.32px;text-transform:uppercase;margin-bottom:8px">{label}</div>
  <div style="color:{TEXT_PRIMARY};font-size:24px;font-weight:300;
              font-family:{FONT_MONO};line-height:1.17">{value}{trend_html}</div>
  {sub_html}
</div>"""


def map_kpi(
    label: str,
    value: str,
    trend: str = "",
    color: str = BLUE_40,
) -> str:
    """
    Compact KPI card for use as Folium DivIcon marker.
    Same Carbon styling but sized for map overlay.
    """
    return f"""
<div style="background:{LAYER_01};border-bottom:2px solid {color};
            padding:6px 10px;font-family:{FONT_SANS};border-radius:0;
            white-space:nowrap;box-shadow:0 2px 6px rgba(0,0,0,0.5)">
  <div style="color:{TEXT_SECONDARY};font-size:9px;font-weight:400;
              letter-spacing:0.32px;text-transform:uppercase">{label}</div>
  <div style="color:{TEXT_PRIMARY};font-size:14px;font-weight:400;
              font-family:{FONT_MONO};line-height:1.2">{value}
    <span style="color:{color};font-size:11px">{trend}</span>
  </div>
</div>"""


def badge(text: str, color: str = C_NORMAL) -> str:
    """
    Carbon Tag/Label component.
    24px border-radius (sole rounded exception in Carbon).
    Background = color at ~15% opacity.
    """
    # Convert hex color to rgba for background
    r = int(color[1:3], 16)
    g = int(color[3:5], 16)
    b = int(color[5:7], 16)
    return (f'<span style="background:rgba({r},{g},{b},0.15);color:{color};'
            f'padding:4px 12px;border-radius:24px;font-size:12px;font-weight:400;'
            f'font-family:{FONT_SANS};letter-spacing:0.32px;display:inline-block">'
            f'{text}</span>')


def section_label(text: str) -> str:
    """Carbon caption-style section label."""
    return (f'<div style="font-family:{FONT_SANS};font-size:12px;font-weight:400;'
            f'letter-spacing:0.32px;color:{TEXT_SECONDARY};text-transform:uppercase;'
            f'margin:24px 0 8px;padding-bottom:8px;border-bottom:1px solid {BORDER}">'
            f'{text}</div>')


def status_color(alert: str) -> str:
    """Map alert level string to Carbon status color."""
    return {
        "critical": C_CRITICAL,
        "watch":    C_WATCH,
        "normal":   C_NORMAL,
        "low_flow": C_LOW_FLOW,
        "no_data":  C_NODATA,
    }.get(alert, C_NODATA)


def status_label(alert: str) -> str:
    """Human-readable alert label."""
    return {
        "critical": "Flood warning",
        "watch":    "Flood watch",
        "normal":   "Normal",
        "low_flow": "Low flow",
        "no_data":  "No data",
        "full":     "Full",
    }.get(alert, alert.replace("_", " ").title())
