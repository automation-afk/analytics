"""Generate Script Scoring System  - Implementation Status Report PDF."""
from fpdf import FPDF
from datetime import datetime


class StatusReport(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, 'Script Scoring System - Implementation Status Report', align='R')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

    def section_title(self, title):
        self.set_font('Helvetica', 'B', 16)
        self.set_text_color(30, 30, 30)
        self.cell(0, 12, title)
        self.ln(8)
        # underline
        self.set_draw_color(52, 152, 219)
        self.set_line_width(0.8)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(6)

    def sub_title(self, title):
        self.set_font('Helvetica', 'B', 13)
        self.set_text_color(50, 50, 50)
        self.cell(0, 10, title)
        self.ln(8)

    def body_text(self, text):
        self.set_font('Helvetica', '', 10)
        self.set_text_color(60, 60, 60)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def status_badge(self, status):
        """Draw a colored status badge."""
        if status == 'DONE':
            bg = (39, 174, 96)
            label = 'DONE'
        elif status == 'PARTIAL':
            bg = (243, 156, 18)
            label = 'PARTIAL'
        else:
            bg = (231, 76, 60)
            label = 'NOT STARTED'
        self.set_fill_color(*bg)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 8)
        w = self.get_string_width(label) + 6
        self.cell(w, 6, label, fill=True, align='C')
        self.set_text_color(60, 60, 60)

    def feature_row(self, feature, status, notes=''):
        """Render a feature row with status badge and notes."""
        y_start = self.get_y()
        if y_start > 260:
            self.add_page()
            y_start = self.get_y()

        # Feature name
        self.set_font('Helvetica', '', 10)
        self.set_text_color(40, 40, 40)
        self.cell(75, 7, feature, border=0)

        # Status badge
        self.status_badge(status)

        # Notes
        x_after_badge = self.get_x() + 3
        self.set_xy(x_after_badge, y_start)
        self.set_font('Helvetica', '', 9)
        self.set_text_color(100, 100, 100)
        remaining_w = self.w - self.r_margin - x_after_badge
        if notes:
            self.multi_cell(remaining_w, 5, notes)
        else:
            self.ln(7)
        self.ln(2)

    def table_header(self, cols, widths):
        self.set_fill_color(44, 62, 80)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 9)
        for col, w in zip(cols, widths):
            self.cell(w, 8, col, border=0, fill=True, align='C')
        self.ln()
        self.set_text_color(60, 60, 60)

    def table_row(self, cells, widths, status_col=None):
        y = self.get_y()
        if y > 265:
            self.add_page()
        self.set_font('Helvetica', '', 9)
        for i, (cell, w) in enumerate(zip(cells, widths)):
            if status_col is not None and i == status_col:
                # Color the status cell
                if cell == 'DONE':
                    self.set_text_color(39, 174, 96)
                    self.set_font('Helvetica', 'B', 9)
                elif cell == 'PARTIAL':
                    self.set_text_color(243, 156, 18)
                    self.set_font('Helvetica', 'B', 9)
                else:
                    self.set_text_color(231, 76, 60)
                    self.set_font('Helvetica', 'B', 9)
                self.cell(w, 7, cell, border=0, align='C')
                self.set_text_color(60, 60, 60)
                self.set_font('Helvetica', '', 9)
            else:
                self.cell(w, 7, cell, border=0)
        self.ln()
        # light separator
        self.set_draw_color(220, 220, 220)
        self.set_line_width(0.2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())


def generate():
    pdf = StatusReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # ========== TITLE PAGE ==========
    pdf.ln(20)
    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(44, 62, 80)
    pdf.cell(0, 15, 'Script Scoring System', align='C')
    pdf.ln(14)
    pdf.set_font('Helvetica', '', 18)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, 'Implementation Status Report', align='C')
    pdf.ln(20)

    # Meta info
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, f'Generated: {datetime.now().strftime("%B %d, %Y")}', align='C')
    pdf.ln(7)
    pdf.cell(0, 7, 'Based on: Script Scoring System - Project Brief (Feb 12, 2026)', align='C')
    pdf.ln(7)
    pdf.cell(0, 7, 'Owner: Joseph  |  Stakeholders: Brandon, Manu, Zach', align='C')
    pdf.ln(20)

    # Summary box
    pdf.set_fill_color(236, 240, 241)
    pdf.set_draw_color(189, 195, 199)
    y_box = pdf.get_y()
    pdf.rect(pdf.l_margin, y_box, pdf.w - pdf.l_margin - pdf.r_margin, 50, style='DF')
    pdf.set_xy(pdf.l_margin + 5, y_box + 5)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.set_text_color(44, 62, 80)
    pdf.cell(0, 7, 'Executive Summary')
    pdf.ln(9)
    pdf.set_x(pdf.l_margin + 5)
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(pdf.w - pdf.l_margin - pdf.r_margin - 10, 5.5,
        'Phases 1-3 of the Script Scoring System are FULLY IMPLEMENTED and deployed. '
        'This includes Gate Checks (6 gates), Quality Score (6 dimensions / 100 points), '
        'Context Multiplier, Rizz Score (vocal + copy), Library View, Trend View, and '
        'Optimization Opportunity. Phases 4-5 (Competitor Scoring, Correlation Analysis), '
        'Calibration Workflow, and the Rizz Confidence Curve visualization remain to be built.')
    pdf.set_y(y_box + 55)

    # Progress bar visual
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(44, 62, 80)
    pdf.cell(0, 8, 'Overall Progress:  ~75% Complete')
    pdf.ln(8)
    bar_w = pdf.w - pdf.l_margin - pdf.r_margin
    # Background
    pdf.set_fill_color(220, 220, 220)
    pdf.rect(pdf.l_margin, pdf.get_y(), bar_w, 8, style='F')
    # Fill
    pdf.set_fill_color(39, 174, 96)
    pdf.rect(pdf.l_margin, pdf.get_y(), bar_w * 0.75, 8, style='F')
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(pdf.l_margin + 2, pdf.get_y() + 1)
    pdf.cell(bar_w * 0.75, 6, '75%', align='C')
    pdf.ln(15)

    # ========== PAGE 2: PHASE-BY-PHASE OVERVIEW ==========
    pdf.add_page()
    pdf.section_title('Phase-by-Phase Overview')

    phases = [
        ('Phase 1: Gate Checks + Brand Alignment', 'DONE',
         'All 6 gate checks implemented: Brand Alignment, Partner Safety, Cross-Video Coherence, '
         'SEO Title Compliance, Funnel Match, Factual Accuracy. Pass/fail table with specific failure '
         'descriptions displayed on video detail page. Approved brands and partner list managed via API endpoints.'),
        ('Phase 2: Transcript-Based Quality Score', 'DONE',
         'All 6 quality dimensions scored from transcript + metadata:\n'
         '  1. Specificity & Proof Density (0-25 pts) - 5 sub-scores\n'
         '  2. Conversion Architecture (0-20 pts) - 5 sub-scores\n'
         '  3. Retention Architecture (0-20 pts) - 5 sub-scores\n'
         '  4. Authenticity & Natural Voice (0-15 pts) - 5 sub-scores\n'
         '  5. Viewer Sophistication Respect (0-10 pts) - 4 sub-scores\n'
         '  6. Production Standards (0-10 pts) - 3 sub-scores\n'
         'Context Multiplier applied based on keyword tier + domination score (0.5x-1.5x). '
         'Library view with sortable table, filters, and Optimization Opportunity formula. '
         'Trend view with Chart.js line chart by channel over time.'),
        ('Phase 3: Rizz Score + Hume Integration', 'DONE',
         'Rizz scoring fully implemented: 60% vocal (Hume AI) + 40% copy (transcript).\n'
         'Vocal sub-metrics (5): Conviction score, Conviction consistency, CTA conviction delta, '
         'Filler word density, Pacing variation.\n'
         'Copy sub-metrics (4): Personality density (via Claude API), Sentence variation, '
         'Decisive language ratio, First-person experience ratio.\n'
         'Graceful degradation when no Hume data available (copy-only score).\n'
         'Displayed on video detail page with color-coded badge and stacked progress bar.'),
        ('Phase 4: Competitor Scoring', 'NOT STARTED',
         'PDF spec: "Extend the system to score competitor videos on the same keywords. '
         'Output: side-by-side comparison (our video vs. top-ranking competitor)."\n'
         'Requires: Fetching competitor transcripts via YouTube Transcript API, scoring them with '
         'the same quality rubric, and displaying a side-by-side comparison view.'),
        ('Phase 5: Correlation Analysis', 'NOT STARTED',
         'PDF spec: "Once we have scores + revenue data + retention data (from YouTube API), '
         'start correlating: which dimensions predict revenue? Does rizz predict retention? '
         'Does frame control predict conversion rate?"\n'
         'This turns the scoring system from a rubric into a predictive model. '
         'Requires: Retention data from YouTube API, regression analysis, dimension reweighting.'),
    ]

    for title, status, desc in phases:
        if pdf.get_y() > 220:
            pdf.add_page()
        pdf.sub_title(title)
        # status badge
        pdf.set_x(pdf.l_margin)
        pdf.status_badge(status)
        pdf.ln(6)
        pdf.body_text(desc)
        pdf.ln(4)

    # ========== PAGE 3: DETAILED FEATURE CHECKLIST ==========
    pdf.add_page()
    pdf.section_title('Detailed Feature Checklist')

    pdf.sub_title('Layer 1: Gate Checks (Binary Pass/Fail)')
    widths = [70, 25, 95]
    pdf.table_header(['Gate Check', 'Status', 'Implementation Notes'], widths)
    gates = [
        ('Brand Alignment', 'DONE', 'Checks CTA vs approved brand list + description match'),
        ('Partner Safety', 'DONE', 'AI scans for negative sentiment near partner names'),
        ('Cross-Video Coherence', 'DONE', 'AI compares transcript vs sibling video transcripts'),
        ('SEO Title Compliance', 'DONE', 'Deterministic keyword + format check on title'),
        ('Funnel Match', 'DONE', 'AI flags education >90s on BOFU keywords'),
        ('Factual Accuracy', 'DONE', 'AI flags specific numbers/claims for manual review'),
    ]
    for name, status, notes in gates:
        pdf.table_row([name, status, notes], widths, status_col=1)
    pdf.ln(8)

    pdf.sub_title('Layer 2: Quality Score (6 Dimensions, 100 pts)')
    widths = [75, 20, 25, 70]
    pdf.table_header(['Dimension', 'Points', 'Status', 'Sub-Scores'], widths)
    dims = [
        ('1. Specificity & Proof Density', '25', 'DONE', 'Quantified claims, F>B>S, proof/min, generic%, LLM smell'),
        ('2. Conversion Architecture', '20', 'DONE', 'CTA placement, frame control, decisiveness, risk reversal, leakage'),
        ('3. Retention Architecture', '20', 'DONE', 'Hook specificity, payoff timing, chapters, section order, reveal'),
        ('4. Authenticity & Natural Voice', '15', 'DONE', 'Anecdotes, personality, scripted vs natural, LLM tone, 2030 test'),
        ('5. Viewer Sophistication', '10', 'DONE', 'Cognitive load, funnel depth, insider signals, reddit-skeptic'),
        ('6. Production Standards', '10', 'DONE', 'B-roll refs, visual evidence, screen capture hygiene'),
    ]
    for name, pts, status, notes in dims:
        pdf.table_row([name, pts, status, notes], widths, status_col=2)
    pdf.ln(8)

    pdf.sub_title('Layer 3: Context Multiplier')
    widths = [85, 25, 80]
    pdf.table_header(['Feature', 'Status', 'Notes'], widths)
    ctx = [
        ('Keyword tier lookup (T1/T2/AI)', 'DONE', 'From BigQuery + domination score'),
        ('Multiplier calculation (0.5x-1.5x)', 'DONE', '5 buckets per PDF spec'),
        ('Quality floor enforcement', 'DONE', '50-80 floor based on tier'),
        ('Optimization Opportunity formula', 'DONE', '(RevPotential - CurrentRev) x (100 - Quality) / 100'),
        ('Multiplied score display', 'DONE', 'Shown in video detail + library view'),
    ]
    for name, status, notes in ctx:
        pdf.table_row([name, status, notes], widths, status_col=1)

    # ========== RIZZ SCORE ==========
    pdf.add_page()
    pdf.sub_title('Rizz Score (Phase 3)')
    widths = [85, 25, 80]
    pdf.table_header(['Component', 'Status', 'Notes'], widths)
    rizz = [
        ('A. Vocal Analysis (60% weight)', 'DONE', 'From Hume AI emotion data'),
        ('  - Vocal conviction score', 'DONE', 'Avg confidence (Determination+Concentration+Interest)'),
        ('  - Conviction consistency', 'DONE', 'Std dev of confidence curve, optimal 0.08-0.12'),
        ('  - CTA conviction delta', 'DONE', 'Last 20% vs overall average'),
        ('  - Filler word density', 'DONE', 'Regex patterns, sweet spot 1-3/min'),
        ('  - Pacing variation', 'DONE', 'Coefficient of variation across chunks'),
        ('B. Copy Analysis (40% weight)', 'DONE', 'From transcript text'),
        ('  - Personality density', 'DONE', '1 Claude API call for humor/callbacks/metaphors'),
        ('  - Sentence length variation', 'DONE', 'CV of sentence word counts'),
        ('  - Decisive language ratio', 'DONE', 'Decisive vs hedging phrase regex matching'),
        ('  - First-person experience ratio', 'DONE', 'First-person vs generic product phrases'),
        ('Graceful degradation (no Hume)', 'DONE', 'Copy-only score when emotions unavailable'),
        ('Interpretation labels', 'DONE', '80-100 Magnetic, 60-79 Solid, 40-59 Flat, <40 Monotone'),
        ('Confidence curve visualization', 'NOT DONE', 'Chart of vocal conviction over video timeline'),
    ]
    for name, status, notes in rizz:
        pdf.table_row([name, status, notes], widths, status_col=1)
    pdf.ln(8)

    # ========== DASHBOARD FEATURES ==========
    pdf.sub_title('Output & Dashboard')
    widths = [85, 25, 80]
    pdf.table_header(['Feature', 'Status', 'Notes'], widths)
    dash = [
        ('Per-Video View', '', ''),
        ('  Gate check results table', 'DONE', '6 gates with pass/fail + descriptions'),
        ('  Quality score (total + 6 bars)', 'DONE', 'Progress bars per dimension + tooltips'),
        ('  Context multiplier display', 'DONE', 'Tier, domination, multiplier, floor, weighted'),
        ('  Rizz score display', 'DONE', 'Badge + stacked bar (vocal/copy)'),
        ('  Rizz confidence curve graph', 'NOT DONE', 'PDF: "overlaid on video timeline"'),
        ('  Top 3 action items', 'DONE', 'Auto-generated from lowest dimensions'),
        ('  Optimization Opportunity', 'DONE', 'Formula with tooltip showing breakdown'),
        ('Library View (Bulk)', '', ''),
        ('  Sortable table', 'DONE', 'All columns sortable, 50/page pagination'),
        ('  Columns per PDF spec', 'DONE', 'Video, Channel, Keyword, Quality, Rizz, Rev, etc.'),
        ('  Default sort by Opt. Opportunity', 'DONE', 'RevPotential x (100-Quality)/100'),
        ('  Filters (channel/silo/keyword/gates)', 'DONE', 'Dropdown + search + toggle'),
        ('Trend View', '', ''),
        ('  Quality avg over time by channel', 'DONE', 'Chart.js line chart with channel toggles'),
        ('  Summary stats', 'DONE', 'Channels, months, data points, overall avg'),
        ('Legacy score de-emphasis', 'DONE', 'Old Script Analysis labeled as Legacy'),
    ]
    for name, status, notes in dash:
        if not status:
            # Section header row
            pdf.set_font('Helvetica', 'B', 9)
            pdf.set_text_color(44, 62, 80)
            pdf.cell(sum(widths), 7, name, border=0)
            pdf.ln()
            pdf.set_draw_color(220, 220, 220)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            continue
        pdf.table_row([name, status, notes], widths, status_col=1)

    # ========== NOT YET IMPLEMENTED ==========
    pdf.add_page()
    pdf.section_title('Not Yet Implemented')

    pdf.sub_title('Phase 4: Competitor Scoring')
    pdf.body_text(
        'PDF Brief (page 10-11):\n'
        '"Extend the system to score competitor videos on the same keywords. '
        'Output: side-by-side comparison (our video vs. top-ranking competitor)."\n\n'
        'What it requires:\n'
        '- Fetch competitor video transcripts via YouTube Transcript API (on-demand)\n'
        '- Score competitor videos using the same 6-dimension quality rubric\n'
        '- Side-by-side comparison view on the video detail page\n'
        '- Identify which dimensions our video wins/loses on vs the competitor\n'
        '- Store competitor scores for trend analysis\n\n'
        'Complexity: MEDIUM  - scoring logic already exists, main work is transcript fetching, '
        'a new comparison UI, and identifying the "top-ranking competitor" per keyword.'
    )

    pdf.sub_title('Phase 5: Correlation Analysis')
    pdf.body_text(
        'PDF Brief (page 11):\n'
        '"Once we have scores + revenue data + retention data (from YouTube API), start correlating: '
        'which dimensions predict revenue? Does rizz predict retention? Does frame control predict '
        'conversion rate? This is where the scoring system stops being a rubric and starts being '
        'a predictive model."\n\n'
        'What it requires:\n'
        '- YouTube Analytics API integration for retention curves (new data source)\n'
        '- Statistical regression: score dimensions vs EPV (earnings per view)\n'
        '- Correlation matrix visualization (heatmap or scatter plots)\n'
        '- Dimension reweighting based on actual revenue correlation\n'
        '- Dashboard page showing "which dimensions matter most"\n\n'
        'Complexity: HIGH  - requires new YouTube API integration, statistical analysis, '
        'and enough scored videos (50+) to produce meaningful correlations.'
    )

    pdf.sub_title('Calibration Workflow')
    pdf.body_text(
        'PDF Brief (page 11):\n'
        '"1. Score 20-30 videos manually (Brandon + Manu) using this rubric. These become the calibration set.\n'
        '2. Compare automated scores to manual scores. Where they diverge, investigate.\n'
        '3. Correlate scores with revenue. After 60-90 days, run regression: which dimensions actually '
        'predict higher EPV? Reweight accordingly.\n'
        '4. Update the LLM smell phrase list monthly."\n\n'
        'What it requires:\n'
        '- Manual scoring interface (form for human scorers to enter dimension scores)\n'
        '- Comparison view: automated score vs manual score, per dimension\n'
        '- Divergence report highlighting where auto and manual disagree\n'
        '- Revenue correlation after 60-90 days of data\n'
        '- LLM smell phrase management UI (add/remove flagged phrases)\n\n'
        'Complexity: MEDIUM  - mostly UI + storage for manual scores and a comparison report.'
    )

    if pdf.get_y() > 200:
        pdf.add_page()

    pdf.sub_title('Rizz Confidence Curve Visualization')
    pdf.body_text(
        'PDF Brief (page 7, 9):\n'
        '"Use Hume\'s voice analysis to generate a confidence curve across the full video timeline. '
        'This is a single line graph (0-100) showing vocal conviction at each moment."\n\n'
        'Page 9: "Rizz score -- total + the confidence curve graph overlaid on video timeline"\n\n'
        'What it requires:\n'
        '- Extract per-segment confidence scores from stored Hume emotion data\n'
        '- Map segment timestamps to video timeline (0% to 100%)\n'
        '- Render a Chart.js line chart in the Rizz section of the video detail page\n'
        '- Overlay CTA locations on the chart to show conviction at CTA moments\n'
        '- Show threshold lines (e.g. "average" level, "ideal" zone)\n\n'
        'Complexity: LOW  - Hume data already stored in DB, just needs a Chart.js visualization. '
        'This is the easiest remaining item to implement.\n\n'
        'Current state: Rizz score IS calculated and displayed as a number + badge. '
        'The vocal sub-metrics are computed from the Hume segments. What\'s missing is only '
        'the visual CHART rendering of the confidence curve over time.'
    )

    # ========== SUMMARY TABLE ==========
    pdf.add_page()
    pdf.section_title('Summary: Implementation Scorecard')
    pdf.ln(4)

    widths = [80, 30, 30, 50]
    pdf.table_header(['Feature Area', 'Status', 'Coverage', 'Key Files'], widths)
    summary = [
        ('Gate Checks (6 gates)', 'DONE', '100%', 'script_scoring_service.py'),
        ('Quality Score (6 dimensions)', 'DONE', '100%', 'script_scoring_service.py'),
        ('Context Multiplier', 'DONE', '100%', 'script_scoring_service.py'),
        ('Optimization Opportunity', 'DONE', '100%', 'videos.py, dashboard.py'),
        ('Rizz Score (vocal + copy)', 'DONE', '100%', 'script_scoring_service.py'),
        ('Per-Video Display', 'DONE', '95%', 'detail.html'),
        ('Library View', 'DONE', '100%', 'script_scores.html'),
        ('Trend View', 'DONE', '100%', 'script_scores_trends.html'),
        ('Action Items (Top 3)', 'DONE', '100%', 'detail.html'),
        ('Legacy Score De-emphasis', 'DONE', '100%', 'detail.html'),
        ('Approved Brands / Partners API', 'DONE', '100%', 'api.py, local_db_service.py'),
        ('Nav Links + Routing', 'DONE', '100%', 'base.html, dashboard.py'),
        ('Hover Tooltips (all sections)', 'DONE', '100%', 'detail.html, overview.html'),
        ('Confidence Curve Chart', 'NOT DONE', '0%', '(needs Chart.js in detail.html)'),
        ('Competitor Scoring (Phase 4)', 'NOT DONE', '0%', '(new service + UI needed)'),
        ('Correlation Analysis (Phase 5)', 'NOT DONE', '0%', '(new data + stats needed)'),
        ('Calibration Workflow', 'NOT DONE', '0%', '(new UI + storage needed)'),
    ]
    for name, status, coverage, files in summary:
        pdf.table_row([name, status, coverage, files], widths, status_col=1)

    pdf.ln(10)

    # Counts
    done_count = sum(1 for _, s, _, _ in summary if s == 'DONE')
    total_count = len(summary)
    not_done = total_count - done_count
    pct = round(done_count / total_count * 100)

    pdf.set_font('Helvetica', 'B', 12)
    pdf.set_text_color(44, 62, 80)
    pdf.cell(0, 10, f'Score: {done_count}/{total_count} features complete ({pct}%)')
    pdf.ln(12)

    # Visual summary boxes
    box_w = (pdf.w - pdf.l_margin - pdf.r_margin - 10) / 3
    y = pdf.get_y()

    # Done box
    pdf.set_fill_color(39, 174, 96)
    pdf.rect(pdf.l_margin, y, box_w, 30, style='F')
    pdf.set_xy(pdf.l_margin, y + 5)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(box_w, 10, str(done_count), align='C')
    pdf.set_xy(pdf.l_margin, y + 18)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(box_w, 8, 'Completed', align='C')

    # Not done box
    pdf.set_fill_color(231, 76, 60)
    pdf.rect(pdf.l_margin + box_w + 5, y, box_w, 30, style='F')
    pdf.set_xy(pdf.l_margin + box_w + 5, y + 5)
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(box_w, 10, str(not_done), align='C')
    pdf.set_xy(pdf.l_margin + box_w + 5, y + 18)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(box_w, 8, 'Remaining', align='C')

    # Percentage box
    pdf.set_fill_color(52, 152, 219)
    pdf.rect(pdf.l_margin + 2 * (box_w + 5), y, box_w, 30, style='F')
    pdf.set_xy(pdf.l_margin + 2 * (box_w + 5), y + 5)
    pdf.set_font('Helvetica', 'B', 24)
    pdf.cell(box_w, 10, f'{pct}%', align='C')
    pdf.set_xy(pdf.l_margin + 2 * (box_w + 5), y + 18)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(box_w, 8, 'Complete', align='C')

    pdf.set_y(y + 40)
    pdf.set_text_color(60, 60, 60)

    # ========== RECOMMENDED NEXT STEPS ==========
    pdf.ln(5)
    pdf.sub_title('Recommended Build Order for Remaining Items')
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(60, 60, 60)

    steps = [
        ('1. Confidence Curve Chart (LOW effort)',
         'Hume emotion data is already stored. Just needs a Chart.js line chart in the Rizz '
         'section of detail.html. Could be done in 1-2 hours.'),
        ('2. Calibration Workflow (MEDIUM effort)',
         'Build manual scoring form + comparison report. Needed before correlation analysis '
         'can produce meaningful results. Requires 20-30 manually scored videos.'),
        ('3. Competitor Scoring - Phase 4 (MEDIUM effort)',
         'Reuse existing scoring pipeline on competitor transcripts. Main work: YouTube transcript '
         'fetching + side-by-side comparison UI. Independently useful for content strategy.'),
        ('4. Correlation Analysis - Phase 5 (HIGH effort)',
         'Requires: enough scored videos (50+), retention data from YouTube API, statistical '
         'regression. Should wait until Phases 1-3 have been running for 60-90 days.'),
    ]
    for title, desc in steps:
        if pdf.get_y() > 250:
            pdf.add_page()
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(44, 62, 80)
        pdf.cell(0, 7, title)
        pdf.ln(7)
        pdf.set_font('Helvetica', '', 10)
        pdf.set_text_color(80, 80, 80)
        pdf.multi_cell(0, 5.5, desc)
        pdf.ln(4)

    # Save
    output_path = 'Script Scoring System - Implementation Status Report.pdf'
    pdf.output(output_path)
    print(f'PDF generated: {output_path}')
    return output_path


if __name__ == '__main__':
    generate()
