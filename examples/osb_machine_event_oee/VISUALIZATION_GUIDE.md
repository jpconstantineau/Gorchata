# Visualization Guide - OSB Machine Event to OEE Analytics

Dashboard design recommendations, chart type selections, and color coding standards for effective data storytelling and decision-making.

## Table of Contents
1. [Dashboard Layouts](#dashboard-layouts)
2. [Chart Type Recommendations](#chart-type-recommendations)
3. [Color Coding Standards](#color-coding-standards)
4. [Tool-Specific Guidance](#tool-specific-guidance)
5. [Best Practices](#best-practices)

---

## Dashboard Layouts

### 1. Executive Summary Dashboard

**Audience:** Plant Manager, Operations Director

**Purpose:** High-level plant performance overview with critical KPIs and trends.

**Update Frequency:** Real-time (or every 15 minutes)

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ OSB Plant Performance - Executive Summary                      │
├──────────────────┬──────────────────┬──────────────────────────┤
│ Plant OEE        │ Availability     │ Performance              │
│ 82.5%            │ 89.2%            │ 94.1%                    │
│ ▲ +2.3% vs LW    │ ▲ +1.5% vs LW    │ ▼ -0.8% vs LW            │
├──────────────────┴──────────────────┴──────────────────────────┤
│ Production vs Target (Today)                                   │
│ ███████████████████████░░░ 240 / 270 tons (89%)                │
├────────────────────────────────────────────────────────────────┤
│ Top 3 Issues (Last 24 Hours)                                   │
│ 1. DRYER-01 Bearing Vibration: 6.2h downtime                   │
│ 2. PRESS-01 Hydraulic Leak: 2.4h downtime                      │
│ 3. Low Green Bins Buffer: 3 starvation events                  │
├────────────────────────────────────────────────────────────────┤
│ 30-Day OEE Trend                                               │
│ 90% ┤                                            ╭──            │
│ 85% ┤                                    ╭───────╯              │
│ 80% ┤                        ╭───────────╯                      │
│ 75% ┤            ╭───────────╯                                  │
│ 70% ┤────────────╯                                              │
│     └──────────────────────────────────────────────────────────┤
│       Feb 15        Feb 22        Mar 1         Mar 8   Today  │
└────────────────────────────────────────────────────────────────┘
```

**Key Metrics:**
- Plant OEE (KPI card, large font, color-coded Green/Yellow/Red)
- A×P×Q Components (KPI cards with vs Last Week trend arrows)
- Daily Production vs Target (horizontal bar gauge)
- Top 3 Issues (prioritized list with impact)
- 30-Day OEE Trend (line chart with target line at 85%)

**Data Sources:**
- `fact_equipment_daily_oee` (aggregated across critical equipment)
- `equipment_downtime_analysis` (top issues)
- Historical OEE (30-day rolling window)

---

### 2. Equipment Performance Dashboard

**Audience:** Reliability Engineer, Maintenance Planner

**Purpose:** Detailed equipment-level OEE, reliability metrics, and failure mode analysis.

**Update Frequency:** Hourly

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ Equipment Performance Analysis                                 │
├────────────────────────────────────────────────────────────────┤
│ OEE Heatmap (Last 7 Days)                                      │
│                Mon   Tue   Wed   Thu   Fri   Sat   Sun         │
│ DRYER-01       🟩92  🟩88  🟨75  🟩90  🟩86  🟩91  🟩89         │
│ PRESS-01       🟩93  🟩90  🟩91  🟩87  🟩92  🟩89  🟩90         │
│ STRAND-01      🟩88  🟨82  🟩85  🟨78  🟩86  🟩87  🟩90         │
│ STRAND-02      🟩90  🟩88  🟩91  🟩89  🟩87  🟩90  🟩92         │
│ FORMER-01      🟨80  🟨77  🟨79  🟨81  🟨83  🟩85  🟩86         │
├────────────────────────────────────────────────────────────────┤
│ OEE Loss Waterfall (DRYER-01, Last 30 Days)                    │
│ 100% ██                                                         │
│  95% ██                                                         │
│  90% ██                                                         │
│  85% ██          ────────  Target Line (85%)                    │
│  80% ██           ┌──┐                                          │
│  75% ██           │  │                                          │
│  70% ██  ┌──┐     │  │     ┌──┐                                │
│  65% ██  │  │  ┌──┤  ├──┐  │  │  ┌──┐                          │
│       ██  │A │  │P │Q │  │  │  │  │OEE                          │
│           │89│  │94│98│  │     │  │82                           │
│           └──┘  └──┴──┴──┘     └──┘                            │
│          100%   -11% -6% -1%   = 82%                            │
├────────────────────────────────────────────────────────────────┤
│ Equipment Reliability Table                                    │
│ Equipment   │ OEE  │ MTBF  │ MTTR │ Failures │ Status          │
│ DRYER-01    │ 82%  │ 48h   │ 2.4h │ 5        │ 🔴 Action Req'd │
│ PRESS-01    │ 90%  │ 140h  │ 3.0h │ 3        │ 🟢 Good         │
│ STRAND-01   │ 85%  │ 90h   │ 1.8h │ 4        │ 🟡 Monitor      │
└────────────────────────────────────────────────────────────────┘
```

**Key Visuals:**
1. **OEE Heatmap** (Equipment × Date Grid)
   - Chart: Heatmap / Matrix
   - Color: Green (≥85%), Yellow (70-84%), Red (<70%)
   - Tooltip: Click for daily details
   - Purpose: Spot performance patterns by equipment/day

2. **OEE Loss Waterfall** (Selected Equipment)
   - Chart: Waterfall chart
   - Bars: 100% → Availability Loss → Performance Loss → Quality Loss → Actual OEE
   - Target line at 85%
   - Purpose: Visualize where losses occur (A vs P vs Q)

3. **Reliability Table**
   - Table with conditional formatting
   - Sort by: Priority (OEE, then MTBF)
   - Status indicators: 🔴 OEE <70%, 🟡 70-84%, 🟢 ≥85%
   - Purpose: Quick equipment health assessment

**Interactivity:**
- Drill-down: Click equipment in heatmap → filter waterfall and table
- Time filter: Last 7 days / Last 30 days / Custom range

---

### 3. Downtime Analysis Dashboard

**Audience:** Maintenance Manager, Reliability Engineer

**Purpose:** Root cause analysis of downtime, failure mode prioritization, and improvement tracking.

**Update Frequency:** Daily

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ Downtime Analysis & Pareto                                     │
├────────────────────────────────────────────────────────────────┤
│ Top 10 Failure Modes (Pareto Chart - Last 30 Days)             │
│                                                      100%       │
│ 50h ┤  ███                                           90%       │
│ 45h ┤  ███                                           80%       │
│ 40h ┤  ███                                           70%       │
│ 35h ┤  ███       ═══════════════════════ 80% Line    60%       │
│ 30h ┤  ███                                           50%       │
│ 25h ┤  ███  ███                                      40%       │
│ 20h ┤  ███  ███  ███                                 30%       │
│ 15h ┤  ███  ███  ███  ███  ███                       20%       │
│ 10h ┤  ███  ███  ███  ███  ███  ███  ███  ███  ███  10%       │
│  5h ┤  ███  ███  ███  ███  ███  ███  ███  ███  ███   0%       │
│     └──┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───           │
│       Bear Hydr Motr Resn Belt Temp Lubr Elec Vibr Other      │
│       Fail Leak Heat Blck Mis  Ctrl Fail Flt  High            │
│       48h  24h  18h  15h  12h  9h   6h   5h   4h   10h         │
│       32%  16%  12%  10%  8%   6%   4%   3%   3%   6%          │
│ Cumul: 32%  48%  60%  70%  78%  84%  88%  91%  94%  100%       │
├────────────────────────────────────────────────────────────────┤
│ MTBF / MTTR Trend (12 Weeks, DRYER-01)                         │
│ 180h ┤                                           ╭──           │
│ 150h ┤                                    ╭──────╯             │
│ 120h ┤                              ╭─────╯                    │
│  90h ┤                       ╭──────╯                          │
│  60h ┤              ╭────────╯   MTBF Improving ✓              │
│  30h ┤──────────────╯                                          │
│   0h └───────────────────────────────────────────────────────  │
│ 4.0h ┤ ●     ●     ●                                           │
│ 3.0h ┤    ●     ●     ●     ●     ●                            │
│ 2.0h ┤                          ●     ●  MTTR Stable           │
│ 1.0h ┤                                     ●     ●             │
│   0h └───────────────────────────────────────────────────────  │
│       W1   W2   W3   W4   W5   W6   W7   W8   W9  W10 W11 W12 │
├────────────────────────────────────────────────────────────────┤
│ Bad Actor Prioritization (Current Period)                      │
│ Rank │ Equipment  │ Criticality │ Impact Score │ Action        │
│ 1    │ DRYER-01   │ Critical    │ 180.0        │ Bearing MON   │
│ 2    │ PRESS-01   │ Critical    │ 36.0         │ Hydraulic PM  │
│ 3    │ STRAND-01  │ Important   │ 18.0         │ Motor Upgrade │
└────────────────────────────────────────────────────────────────┘
```

**Key Visuals:**
1. **Pareto Chart** (Failure Modes)
   - Chart: Combined bar + line chart
   - Bars: Downtime hours by failure mode (descending)
   - Line: Cumulative % (0-100%)
   - Horizontal line: 80% threshold (identify "vital few")
   - Purpose: Focus on top 20% of causes driving 80% of downtime

2. **MTBF/MTTR Trends** (12-Week Rolling)
   - Chart: Dual-axis line chart
   - Line 1 (left axis): MTBF in hours (target: increasing trend)
   - Line 2 (right axis): MTTR in hours (target: <2h, stable)
   - Purpose: Track reliability improvement initiatives

3. **Bad Actor Table**
   - Table sorted by Impact Score (descending)
   - Columns: Rank, Equipment, Criticality, Impact Score, Recommended Action
   - Purpose: Maintenance investment prioritization

**Interactivity:**
- Equipment filter: Select specific equipment or view all
- Time range: Last 30 days / Last 90 days / YTD
- Drill-down: Click Pareto bar → see detailed failure events

---

### 4. Buffer Management Dashboard

**Audience:** Process Engineer, Production Supervisor

**Purpose:** Buffer utilization monitoring, starvation/blocking prediction, constraint identification.

**Update Frequency:** Every 15 minutes (near real-time)

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ Buffer Status & Constraint Analysis                            │
├────────────────────────────────────────────────────────────────┤
│ Buffer Level Time-Series (Last 24 Hours)                       │
│ 10h ┤ Green Bins (Capacity: 4h)                                │
│  8h ┤     ╭─────╮                                               │
│  6h ┤     │     │      ╭────╮                                   │
│  4h ┤ ────┼─────┼──────┼────┼─────────── Optimal Range         │
│  2h ┤     │     ╰──────╯    │                                   │
│  0h ┤─────╯ STARVED (2h)    ╰──────────                         │
│     └───────────────────────────────────────────────────────── │
│       08:00  10:00  12:00  14:00  16:00  18:00  20:00  Today   │
│                                                                 │
│ 10h ┤ Dry Silos (Capacity: 8h)                                 │
│  8h ┤                                         ███ BLOCKED (1h)  │
│  6h ┤                         ╭───────────────███               │
│  4h ┤ ────────────────────────┼────────────── ╯                │
│  2h ┤         ╭───────────────╯                                 │
│  0h ┤─────────╯                                                 │
│     └───────────────────────────────────────────────────────── │
│       08:00  10:00  12:00  14:00  16:00  18:00  20:00  Today   │
├────────────────────────────────────────────────────────────────┤
│ Starvation/Blocking Frequency (Last 7 Days)                    │
│ 20  ┤   █                       Starvation (upstream failure)  │
│ 15  ┤   █       █                                               │
│ 10  ┤   █   █   █   █                                           │
│  5  ┤   █   █   █   █   █   █                                   │
│  0  ┤   █   █   █   █   █   █   █       Blocking (downstream)  │
│     └───┴───┴───┴───┴───┴───┴───                               │
│       Mon Tue Wed Thu Fri Sat Sun                               │
├────────────────────────────────────────────────────────────────┤
│ Constraint Heatmap (TOC Analysis - Current Week)               │
│                Util%  Starve%  Block%  Constraint Score         │
│ DRYER-01       100%   0%       0%      100.0   ← BOTTLENECK    │
│ STRAND-01      95%    0%       12%     95.0                     │
│ PRESS-01       56%    5%       0%      58.8                     │
│ FORMER-01      48%    8%       0%      52.0                     │
└────────────────────────────────────────────────────────────────┘
```

**Key Visuals:**
1. **Buffer Level Time-Series**
   - Chart: Multi-line time-series (one line per buffer)
   - Shaded regions: Red (0 = starved), Green (40-60% optimal), Orange (>80% = near blocking)
   - Annotations: Mark starvation/blocking events
   - Purpose: Real-time buffer monitoring, predict failures

2. **Starvation/Blocking Frequency**
   - Chart: Stacked bar chart (dual direction: up=starvation, down=blocking)
   - Purpose: Identify days/equipment with highest propagation risk

3. **Constraint Heatmap**
   - Chart: Table with color-coded cells
   - Metrics: Utilization %, Starvation %, Blocking %, Constraint Score
   - Color: Red (high), Yellow (medium), Green (low)
   - Sort by: Constraint Score (descending)
   - Purpose: Identify plant bottleneck (TOC)

**Alerts:**
- Buffer <20%: "Starvation Risk - Green Bins depleting"
- Buffer >80%: "Blocking Risk - Dry Silos near capacity"
- Constraint Score change: "PRESS-01 now constraint (DRYER-01 improved)"

---

### 5. Quality Dashboard

**Audience:** Quality Manager, Process Engineer

**Purpose:** Quality defect tracking, root cause correlation, SPC monitoring.

**Update Frequency:** Every shift (3×/day)

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ Quality Metrics & Root Cause Analysis                          │
├────────────────────────────────────────────────────────────────┤
│ Defect Rate Trend (Rolling 7-Day Avg, Last 30 Days)            │
│  6% ┤                                                           │
│  5% ┤ ●───●                                                     │
│  4% ┤       ●───●     UCL (5%)                                  │
│  3% ┤             ●───●───●                                     │
│  2% ┤                       ●───●───● Target (2%)               │
│  1% ┤                                 ●───●───●───● Improved!   │
│  0% └───────────────────────────────────────────────────────── │
│       Mar 1      Mar 8      Mar 15      Mar 22      Today       │
├────────────────────────────────────────────────────────────────┤
│ Thickness Distribution (Last 1000 Panels, Target: 11mm ±0.5mm) │
│ 200 ┤                   ███                                     │
│ 150 ┤                ███████████                                │
│ 100 ┤            ███████████████████                            │
│  50 ┤        ███████████████████████████                        │
│   0 └─────────────────────────────────────────────────────     │
│      10.0  10.3  10.6  10.9  11.2  11.5  11.8   mm             │
│      LSL                 Target        USL                      │
│                                                                 │
│ Density Distribution (Last 1000 Panels, Target: 650 kg/m³ ±30) │
│ 200 ┤                ███                                        │
│ 150 ┤            ███████████                                    │
│ 100 ┤        ███████████████████                                │
│  50 ┤    ███████████████████████████                            │
│   0 └─────────────────────────────────────────────────────     │
│      600   620   640   660   680   700   kg/m³                 │
│      LSL             Target       USL                           │
├────────────────────────────────────────────────────────────────┤
│ Defect Root Cause Correlation                                  │
│ Defect Type           │ Count │ Avg Press Temp │ Action        │
│ Thickness Deviation   │ 15    │ 143°C (LOW)    │ Fix Temp Ctrl │
│ Delamination          │ 5     │ 165°C (OK)     │ Check Resin   │
│ Surface Roughness     │ 3     │ 172°C (OK)     │ Monitor       │
└────────────────────────────────────────────────────────────────┘
```

**Key Visuals:**
1. **Defect Rate Trend** (SPC Chart)
   - Chart: Line chart with control limits
   - Lines: UCL (Upper Control Limit), Target, LCL (Lower Control Limit)
   - Purpose: Monitor quality process stability

2. **Parameter Distributions** (Histograms)
   - Chart: Histogram with normal curve overlay
   - Spec limits: LSL (Lower), Target, USL (Upper) as vertical lines
   - Capability metrics: Cpk displayed
   - Purpose: Assess process centering and spread

3. **Root Cause Correlation Table**
   - Table: Defect type × Process parameters
   - Highlight: Cells where parameter is out-of-spec
   - Purpose: Link defects to process conditions

**Interactivity:**
- Shift filter: Day / Swing / Night
- Product filter: 11mm / 15mm / 18mm OSB
- Drill-down: Click defect type → see individual events with timestamps

---

### 6. Maintenance Dashboard

**Audience:** Maintenance Manager, Reliability Engineer

**Purpose:** Maintenance strategy assessment, PM effectiveness, ROI tracking.

**Update Frequency:** Weekly

**Layout:**
```
┌────────────────────────────────────────────────────────────────┐
│ Maintenance Strategy & Effectiveness                           │
├────────────────────────────────────────────────────────────────┤
│ Bad Actor Prioritization (Impact Score × ROI)                  │
│ Rank │ Equipment  │ Impact │ MTBF  │ ROI %  │ Payback │ Action │
│ 1    │ DRYER-01   │ 180.0  │ 48h   │ 56%    │ 7.7mo   │ ✓ Fund │
│ 2    │ PRESS-01   │ 36.0   │ 140h  │ 18%    │ 25mo    │ Monitor│
│ 3    │ STRAND-01  │ 18.0   │ 90h   │ 12%    │ 38mo    │ Defer  │
├────────────────────────────────────────────────────────────────┤
│ PM Compliance (Target: >50% for Proactive Strategy)            │
│ DRYER-01       ████░░░░░░░░░░░░░░░░  28.6% 🔴 Reactive         │
│ PRESS-01       ██████████░░░░░░░░░░  50.0% 🟢 Proactive        │
│ STRAND-01      ██████████░░░░░░░░░░  50.0% 🟢 Proactive        │
│ FORMER-01      ████████░░░░░░░░░░░░  40.0% 🟡 Mixed            │
├────────────────────────────────────────────────────────────────┤
│ Breakdown vs PM Cost Comparison (Last 90 Days)                 │
│ $18K ┤                DRYER-01                                  │
│ $15K ┤                  ███                                     │
│ $12K ┤                  ███        7.5× ratio (target: <3×)     │
│  $9K ┤  PRESS-01        ███                                     │
│  $6K ┤    ███           ███                                     │
│  $3K ┤    ███    ██     ███    ██                               │
│  $0K └────┴──────┴──────┴──────┴──                             │
│         PM  Brkdn   PM  Brkdn  PM                               │
├────────────────────────────────────────────────────────────────┤
│ MTBF Improvement Trend (with Targets)                          │
│ 200h ┤                                      ╭── Target (180h)  │
│ 160h ┤                               ╭──────╯                   │
│ 120h ┤                        ╭──────╯                          │
│  80h ┤                 ╭──────╯                                 │
│  40h ┤──────────────────╯  DRYER-01 Improvement ✓              │
│   0h └───────────────────────────────────────────────────────  │
│       Week 1    Week 4    Week 8    Week 12   Today            │
└────────────────────────────────────────────────────────────────┘
```

**Key Visuals:**
1. **Bad Actor Table with ROI**
   - Table: Equipment ranked by Impact Score
   - Columns: Rank, Equipment, Impact Score, MTBF, ROI %, Payback Period, Action (Fund/Monitor/Defer)
   - Purpose: Prioritize maintenance budget

2. **PM Compliance Gauge**
   - Chart: Horizontal bar gauge (per equipment)
   - Target line: 50% (proactive strategy threshold)
   - Color: Red (<30%), Yellow (30-50%), Green (>50%)
   - Purpose: Shift from reactive to proactive maintenance

3. **Breakdown vs PM Cost Comparison**
   - Chart: Grouped bar chart (equipment on X-axis, cost on Y-axis)
   - Bars: PM Cost (blue), Breakdown Cost (red)
   - Annotation: Cost ratio (Breakdown / PM)
   - Target: <3× ratio (typical range: 3-10×)
   - Purpose: Justify PM investment

4. **MTBF Improvement Trend**
   - Chart: Line chart with target line
   - Purpose: Track reliability improvement initiatives

**Interactivity:**
- Criticality filter: Critical / Important / Standard
- Sort: By Impact Score / ROI / MTBF

---

## Chart Type Recommendations

### Time-Series Analysis
**Use For:** OEE trends, MTBF evolution, buffer levels, production output

**Chart Types:**
- **Line Chart**: Continuous metrics over time (OEE %, buffer level)
- **Area Chart**: Cumulative metrics (total production)
- **Dual-Axis Line**: Compare different scales (MTBF hours + MTTR hours)

**Best Practices:**
- Add reference lines for targets (e.g., 85% OEE)
- Use rolling averages to smooth noise (7-day, 30-day)
- Mark significant events with annotations (e.g., "PM completed", "Bearing replaced")

---

### Prioritization & Pareto Analysis
**Use For:** Failure mode prioritization, defect analysis, downtime causes

**Chart Types:**
- **Pareto Chart**: Combined bar (descending) + cumulative line (0-100%)
- **Horizontal Bar**: Ranked lists (top 10 bad actors)

**Best Practices:**
- Sort bars descending (largest impact first)
- Add 80% threshold line (identify "vital few")
- Limit to top 10-15 items (avoid clutter)
- Use color to distinguish categories (Availability/Performance/Quality losses)

---

### Comparison Analysis
**Use For:** Shift performance, equipment comparison, before/after analysis

**Chart Types:**
- **Grouped Bar Chart**: Compare multiple metrics side-by-side (PM cost vs Breakdown cost)
- **Stacked Bar Chart**: Show composition (OEE = A + P + Q components)
- **Waterfall Chart**: Show sequential impact (100% → -A → -P → -Q = OEE)

**Best Practices:**
- Limit groups to 3-5 for readability
- Use consistent color palette across charts
- Add data labels for exact values

---

### Distribution Analysis
**Use For:** Quality parameters (thickness, density), process capability

**Chart Types:**
- **Histogram**: Frequency distribution with normal curve overlay
- **Box Plot**: Show median, quartiles, outliers

**Best Practices:**
- Add specification limits (LSL, Target, USL) as vertical lines
- Calculate and display Cpk (process capability index)
- Use 20-30 bins for histograms

---

### Correlation & Root Cause
**Use For:** Quality defect correlation, parameter relationships

**Chart Types:**
- **Scatter Plot**: Show relationship between two variables (temp × defect rate)
- **Heat Map**: Show correlation matrix or multi-dimensional data (shift × equipment × OEE)

**Best Practices:**
- Add trendline/regression line to scatter plots
- Use color gradient for heatmaps (green = good, red = bad)
- Enable drill-down (click cell → see details)

---

### Status & KPIs
**Use For:** Real-time dashboards, executive summaries

**Chart Types:**
- **KPI Card**: Large number with trend arrow (OEE 82.5% ▲ +2.3%)
- **Gauge Chart**: Show progress to target (Production: 240 / 270 tons)
- **Bullet Chart**: Compare actual vs target vs benchmark

**Best Practices:**
- Use large, readable fonts (24-36pt for KPIs)
- Add context (vs Last Week, vs Target)
- Limit to 4-6 KPI cards per dashboard

---

## Color Coding Standards

### OEE Performance Levels
- **🟢 Green (≥85%)**: World-class performance, maintain current practices
- **🟡 Yellow (70-84%)**: Good performance, opportunity for optimization
- **🔴 Red (<70%)**: Needs improvement, immediate action required

### Availability Levels
- **🟢 Green (≥90%)**: Excellent availability
- **🟡 Yellow (80-89%)**: Good availability
- **🔴 Red (<80%)**: Poor availability, investigate downtime

### Performance Levels
- **🟢 Green (≥95%)**: Excellent performance (minimal speed losses)
- **🟡 Yellow (85-94%)**: Good performance
- **🔴 Red (<85%)**: Poor performance, investigate minor stops

### Quality Levels
- **🟢 Green (≥99%)**: Excellent quality
- **🟡 Yellow (97-98%)**: Good quality
- **🔴 Red (<97%)**: Poor quality, investigate defects

### Buffer Status
- **🔴 Red (0-20%)**: Starvation risk, immediate action
- **🟡 Yellow (20-40% or 60-80%)**: Suboptimal, monitor closely
- **🟢 Green (40-60%)**: Optimal buffer level
- **🟠 Orange (80-100%)**: Blocking risk, reduce inflow

### Maintenance Strategy
- **🟢 Green (PM Ratio >50%)**: Proactive strategy
- **🟡 Yellow (PM Ratio 30-50%)**: Mixed strategy
- **🔴 Red (PM Ratio <30%)**: Reactive strategy, increase PM

### Trend Indicators
- **↑ Green Arrow**: Improving (OEE increasing, MTBF increasing, MTTR decreasing)
- **→ Yellow Arrow**: Stable (no significant change)
- **↓ Red Arrow**: Declining (OEE decreasing, MTBF decreasing, MTTR increasing)

### Semantic Colors
- **Blue**: Neutral/informational (planned downtime, PM events)
- **Orange**: Warning (approaching threshold, buffer <20%)
- **Red**: Critical/urgent (breakdown, starvation, OEE <70%)
- **Green**: Good/target (world-class performance, optimal buffer)
- **Gray**: Inactive/excluded (weekend data, non-production time)

---

## Tool-Specific Guidance

### Power BI
**Best For:** Enterprise-scale reporting, self-service BI, interactive dashboards

**Setup:**
1. **Data Model:** Import from SQL with star schema (fact + dimensions)
2. **DAX Measures:**
   ```dax
   Plant OEE = AVERAGE(fact_equipment_daily_oee[oee_pct])
   OEE vs LW = [Plant OEE] - CALCULATE([Plant OEE], DATEADD(dim_date[date_id], -7, DAY))
   ```
3. **Drill-Through:** Equipment Performance dashboard → click DRYER-01 → detailed DRYER-01 page
4. **Row-Level Security:** Filter by plant_id for multi-site deployments

**Recommended Visuals:**
- KPI Cards (built-in)
- Matrix (heatmap with conditional formatting)
- Waterfall Chart (OEE loss breakdown)
- Decomposition Tree (root cause drill-down)

---

### Tableau
**Best For:** Interactive analysis, ad-hoc exploration, storytelling

**Setup:**
1. **Data Source:** Live connection to SQL database or extract (.hyper)
2. **Calculated Fields:**
   ```tableau
   OEE = [Availability %] * [Performance %] * [Quality %] / 10000
   OEE Color = IF [OEE] >= 85 THEN "Green" ELSEIF [OEE] >= 70 THEN "Yellow" ELSE "Red" END
   ```
3. **Dashboard Actions:** Filter, Highlight, URL (drill to detailed report)
4. **Parameters:** Date range, Equipment filter, Shift selection

**Recommended Visuals:**
- Highlight Tables (heatmap)
- Gantt Chart (equipment timeline with states)
- Pareto Chart (dual-axis combo)
- Treemap (equipment by impact score)

---

### Grafana
**Best For:** Real-time monitoring, time-series dashboards, operational displays

**Setup:**
1. **Data Source:** PostgreSQL/MySQL plugin (connect to OEE database)
2. **Variables:** `$equipment`, `$shift`, `$date_range` (dashboard-level filters)
3. **Queries:**
   ```sql
   SELECT $__time(date_id), oee_pct 
   FROM fact_equipment_daily_oee 
   WHERE equipment_id = '$equipment' 
   AND $__timeFilter(date_id)
   ```
4. **Alerts:** Threshold alerts (OEE <70%, Buffer <20%)

**Recommended Panels:**
- Time Series (buffer level, OEE trend)
- Stat (KPI cards with thresholds)
- Table (sorted lists)
- Bar Gauge (PM compliance)

---

### Excel / PowerPoint
**Best For:** Ad-hoc analysis, executive presentations, report distribution

**Setup:**
1. **Data Connection:** Power Query → import from SQL
2. **PivotTables:** Equipment × Date OEE heatmap
3. **Charts:** Standard Excel charts (Line, Column, Waterfall)
4. **Conditional Formatting:** 3-Color Scale (Red-Yellow-Green) for OEE cells

**Recommended Charts:**
- Line Chart with Data Labels (OEE trend)
- Stacked Column (OEE loss breakdown)
- Combo Chart (Pareto: Column + Line)
- Pivot Chart (dynamic filtering)

---

## Best Practices

### Dashboard Design Principles

1. **Hierarchy:** Most important metrics at top-left (F-pattern eye tracking)
2. **Grouping:** Related visuals together (e.g., all OEE components adjacent)
3. **Whitespace:** Avoid clutter, use whitespace to separate sections
4. **Consistency:** Same chart types for same metric across dashboards
5. **Context:** Always include time period, target lines, benchmarks

### Performance Optimization

1. **Pre-Aggregation:** Create summary tables (daily\_plant\_oee) for common queries
2. **Indexes:** Add indexes on `date_id`, `equipment_id`, `shift_id`
3. **Incremental Load:** Only load new/updated data (not full refresh)
4. **Materialized Views:** For complex calculations (constraint_score, impact_score)

### User Experience

1. **Interactivity:** 
   - Reset button (clear all filters)
   - Bookmark views (save common filter states)
   - Tooltips (explain metric calculations on hover)

2. **Mobile Responsiveness:** 
   - Stack visuals vertically for mobile
   - Prioritize KPI cards for small screens
   - Hide detailed tables on mobile (keep charts)

3. **Accessibility:**
   - Use colorblind-safe palettes
   - Add pattern fills (not just color)
   - Provide data table view alongside charts

### Storytelling

1. **Executive Summary:** Start with "So What?" (Plant OEE 82%, need 3% improvement to hit world-class)
2. **Root Cause:** Show drill-down path (Low OEE → Availability issue → Dryer bearing failures)
3. **Action Plan:** End with recommendations (Replace bearing controller: $100K, 7.7mo payback)
4. **Progress Tracking:** Show before/after trends (MTBF improved 40h → 167h over 12 weeks)

### Update Frequency

| Dashboard | Frequency | Rationale |
|-----------|-----------|-----------|
| Executive Summary | Real-time (15 min) | Operations decisions require current data |
| Equipment Performance | Hourly | Balance freshness vs query load |
| Downtime Analysis | Daily | Daily maintenance meetings |
| Buffer Management | Real-time (5 min) | Prevent starvation/blocking |
| Quality | Per shift (3×/day) | Shift handover reporting |
| Maintenance | Weekly | Strategic planning cycle |

---

## Example Dashboard URLs

*Replace with your BI tool URLs after deployment:*

- **Executive Summary:** `https://bi.yourcompany.com/osb/executive`
- **Equipment Performance:** `https://bi.yourcompany.com/osb/equipment`
- **Downtime Analysis:** `https://bi.yourcompany.com/osb/downtime`
- **Buffer Management:** `https://bi.yourcompany.com/osb/buffers`
- **Quality Dashboard:** `https://bi.yourcompany.com/osb/quality`
- **Maintenance Dashboard:** `https://bi.yourcompany.com/osb/maintenance`

---

## Additional Resources

- See [README.md](README.md) for project overview and business context
- See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete table/column documentation
- See [EXAMPLE_QUERIES.md](EXAMPLE_QUERIES.md) for SQL queries powering these dashboards
