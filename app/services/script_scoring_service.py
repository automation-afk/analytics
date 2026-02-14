"""
Script Scoring Service — comprehensive video script evaluation.

Implements the 3-layer scoring system from the Script Scoring System project brief:
  Layer 1: Gate Checks (binary pass/fail)
  Layer 2: Quality Score (weighted 100-point scale, 6 dimensions)
  Layer 3: Context Multiplier (keyword revenue potential adjustment)
  Layer 4: Rizz Score (60% vocal from Hume AI + 40% copy from transcript)

Final Output = Quality Score x Context Multiplier
"""

from anthropic import Anthropic
import json
import math
import re
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

from app.models import ScriptScore, GateCheckResult

logger = logging.getLogger(__name__)

# Context multiplier lookup table
MULTIPLIER_TABLE = {
    # (tier, domination_threshold) -> (multiplier, quality_floor)
    'tier1_low_dom': (1.5, 80),   # Tier 1 priority keyword, domination <90%
    'tier1_high_dom': (1.0, 70),  # Tier 1 priority keyword, domination >90%
    'tier2_proven': (1.2, 70),    # Tier 2 / expansion, proven revenue
    'tier2_unproven': (0.8, 60),  # Tier 2 / expansion, unproven
    'ai_skeleton': (0.5, 50),     # AI/skeleton test video
}

# LLM smell phrases to detect AI-generated content
LLM_SMELL_PHRASES = [
    "here's the reality", "let's dive in", "here's the thing",
    "is a game-changer", "game changer", "in today's 2024", "in today's 2025",
    "in today's 2026", "without further ado", "buckle up",
    "in this comprehensive", "we'll explore", "let's break it down",
    "at the end of the day", "it's important to note", "it goes without saying",
    "in the world of", "when it comes to", "look no further",
]

# Vague credibility phrases (deductions in Specificity dimension)
VAGUE_CREDIBILITY_PHRASES = [
    "extensive testing", "thorough testing", "comprehensive testing",
    "in-depth research", "extensive research", "thorough research",
    "rigorous testing", "exhaustive testing", "meticulous testing",
]

# Objection-naming phrases (flags in Conversion Architecture)
OBJECTION_PHRASES = [
    "you might be wondering", "some people worry", "a common concern is",
    "don't worry about", "you might think", "some might say",
    "a lot of people ask", "one concern is",
]


class ScriptScoringService:
    """Comprehensive script scoring: gates + quality + context multiplier."""

    def __init__(self, api_key: str, local_db, bigquery_service,
                 model: str = "claude-sonnet-4-20250514",
                 prompt_version: str = "1.0"):
        self.client = Anthropic(api_key=api_key)
        self.model = model
        self.local_db = local_db
        self.bigquery = bigquery_service
        self.prompt_version = prompt_version
        logger.info(f"ScriptScoringService initialized (model={model}, prompt_v={prompt_version})")

    def score_video(self, video_id: str, transcript: str, title: str,
                    description: str, duration_seconds: int = 0,
                    progress_callback=None) -> ScriptScore:
        """Full scoring pipeline: gates + quality + context multiplier.

        Args:
            video_id: YouTube video ID
            transcript: Full transcript text
            title: Video title
            description: Video description
            duration_seconds: Video duration in seconds (0 if unknown)
            progress_callback: Optional callback for progress updates

        Returns:
            ScriptScore with all results populated
        """
        now = datetime.now()
        score = ScriptScore(video_id=video_id, scored_at=now, scoring_version=self.prompt_version)

        # Get video context from BigQuery
        if progress_callback:
            progress_callback("Fetching video context...")
        context = self._get_video_context(video_id)

        # --- Layer 1: Gate Checks ---
        if progress_callback:
            progress_callback("Running gate checks...")
        gate_results = self.run_gate_checks(
            video_id, transcript, title, description, context
        )
        score.gate_results = gate_results
        score.all_gates_passed = all(g.passed for g in gate_results)

        # --- Layer 2: Quality Score ---
        if progress_callback:
            progress_callback("Scoring quality dimensions...")
        quality_result = self.score_quality(
            transcript, title, description, duration_seconds, context
        )
        if quality_result:
            score.quality_score_total = quality_result.get('quality_score_total', 0)
            score.specificity_score = quality_result.get('specificity_score', 0)
            score.conversion_arch_score = quality_result.get('conversion_arch_score', 0)
            score.retention_arch_score = quality_result.get('retention_arch_score', 0)
            score.authenticity_score = quality_result.get('authenticity_score', 0)
            score.viewer_respect_score = quality_result.get('viewer_respect_score', 0)
            score.production_score = quality_result.get('production_score', 0)
            score.dimension_details = quality_result.get('dimension_details', {})
            score.action_items = quality_result.get('action_items', [])

        # --- Layer 3: Context Multiplier ---
        if progress_callback:
            progress_callback("Computing context multiplier...")
        multiplier_result = self.compute_context_multiplier(video_id, context)
        score.keyword_tier = multiplier_result.get('keyword_tier')
        score.domination_score = multiplier_result.get('domination_score')
        score.context_multiplier = multiplier_result.get('multiplier', 1.0)
        score.quality_floor = multiplier_result.get('quality_floor', 60)

        if score.quality_score_total is not None:
            score.multiplied_score = round(score.quality_score_total * score.context_multiplier, 1)
            score.passes_quality_floor = score.quality_score_total >= score.quality_floor

        # --- Layer 4: Rizz Score ---
        if progress_callback:
            progress_callback("Scoring Rizz (vocal + copy)...")
        try:
            # Get emotion data from existing transcript
            transcript_data = self.local_db.get_transcript(video_id)
            emotions_data = transcript_data.get('emotions') if transcript_data else None
            if not duration_seconds and transcript_data:
                duration_seconds = transcript_data.get('duration_seconds', 0) or 0

            rizz_result = self.score_rizz(video_id, transcript, emotions_data, duration_seconds)
            score.rizz_score = rizz_result['rizz_score']
            score.rizz_vocal_score = rizz_result['rizz_vocal_score']
            score.rizz_copy_score = rizz_result['rizz_copy_score']
            score.rizz_details = rizz_result['rizz_details']
            logger.info(f"Rizz score: {score.rizz_score} (vocal={score.rizz_vocal_score}, copy={score.rizz_copy_score})")
        except Exception as e:
            logger.error(f"Rizz scoring failed for {video_id}: {str(e)}")

        logger.info(f"Scored video {video_id}: quality={score.quality_score_total}, "
                    f"multiplied={score.multiplied_score}, rizz={score.rizz_score}, "
                    f"gates={'PASS' if score.all_gates_passed else 'FAIL'}")
        return score

    # ========== Layer 1: Gate Checks ==========

    def run_gate_checks(self, video_id: str, transcript: str, title: str,
                        description: str, context: Dict) -> List[GateCheckResult]:
        """Run all 6 gate checks. Returns list of GateCheckResult."""
        results = []

        # Gate 1: Brand Alignment (deterministic)
        results.append(self._check_brand_alignment(video_id, transcript, description, context))

        # Gate 2: SEO Title Compliance (deterministic)
        results.append(self._check_seo_title(title, context))

        # Gates 3-6: AI-assisted (single Claude call)
        ai_gates = self._run_ai_gate_checks(video_id, transcript, title, context)
        results.extend(ai_gates)

        return results

    def _check_brand_alignment(self, video_id: str, transcript: str,
                                description: str, context: Dict) -> GateCheckResult:
        """Gate 1: Is the primary CTA the approved brand for this silo?"""
        silo = context.get('silo', '')
        if not silo:
            return GateCheckResult(
                gate_name="Brand Alignment",
                passed=True,
                failure_reason="No silo assigned — skipped"
            )

        # Check approved_brands table first, fall back to config
        approved = self.local_db.get_approved_brand_for_silo(silo)
        if not approved:
            return GateCheckResult(
                gate_name="Brand Alignment",
                passed=True,
                failure_reason="No approved brand defined for this silo — skipped"
            )

        primary_brand = approved['primary_brand'].lower()
        secondary_brand = (approved.get('secondary_brand') or '').lower()

        # Check if approved brand appears in transcript or description
        combined_text = (transcript + ' ' + description).lower()
        has_primary = primary_brand in combined_text
        has_secondary = secondary_brand in combined_text if secondary_brand else False

        if has_primary:
            return GateCheckResult(gate_name="Brand Alignment", passed=True)

        if has_secondary:
            return GateCheckResult(
                gate_name="Brand Alignment",
                passed=True,
                failure_reason=f"Using secondary brand ({approved.get('secondary_brand')}), not primary ({approved['primary_brand']})"
            )

        return GateCheckResult(
            gate_name="Brand Alignment",
            passed=False,
            failure_reason=f"Approved brand for {silo} silo is {approved['primary_brand']}, "
                          f"but it was not found in the transcript or description"
        )

    def _check_seo_title(self, title: str, context: Dict) -> GateCheckResult:
        """Gate 2: Does the title contain the target parent keyword?"""
        keyword = context.get('main_keyword', '')
        if not keyword:
            return GateCheckResult(
                gate_name="SEO Title Compliance",
                passed=True,
                failure_reason="No target keyword assigned — skipped"
            )

        title_lower = title.lower()
        keyword_lower = keyword.lower()

        # Check if keyword or significant parts appear in title
        if keyword_lower in title_lower:
            return GateCheckResult(gate_name="SEO Title Compliance", passed=True)

        # Check individual significant words (3+ chars) from keyword
        keyword_words = [w for w in keyword_lower.split() if len(w) >= 3]
        matches = sum(1 for w in keyword_words if w in title_lower)
        if keyword_words and matches >= len(keyword_words) * 0.6:
            return GateCheckResult(
                gate_name="SEO Title Compliance",
                passed=True,
                failure_reason=f"Partial keyword match ({matches}/{len(keyword_words)} words)"
            )

        return GateCheckResult(
            gate_name="SEO Title Compliance",
            passed=False,
            failure_reason=f"Target keyword '{keyword}' not found in title '{title}'"
        )

    def _run_ai_gate_checks(self, video_id: str, transcript: str,
                             title: str, context: Dict) -> List[GateCheckResult]:
        """Gates 3-6 via single Claude API call: Partner Safety, Cross-Video Coherence,
        Funnel Match, Factual Accuracy."""

        # Gather data for the prompt
        partners = self.local_db.get_partner_list(active_only=True)
        partner_names = [p['brand_name'] for p in partners] if partners else []

        keyword = context.get('main_keyword', '')
        silo = context.get('silo', '')

        # Get sibling transcripts for coherence check
        sibling_summaries = ""
        if keyword:
            siblings = self.bigquery.get_sibling_video_ids(video_id, keyword, limit=3)
            for sib in siblings:
                sib_transcript = self.bigquery.get_transcript(sib['video_id'])
                if sib_transcript:
                    truncated = sib_transcript[:2000]
                    sibling_summaries += f"\n--- Video: {sib['video_title']} ---\n{truncated}\n"

        # Truncate main transcript for the gate check prompt
        transcript_truncated = transcript[:10000] if len(transcript) > 10000 else transcript

        prompt = f"""You are a video script auditor for an affiliate marketing YouTube channel.
Evaluate this script against the following gate checks. Each gate is PASS or FAIL.

VIDEO TITLE: {title}
TARGET KEYWORD: {keyword or 'Not assigned'}
SILO/CATEGORY: {silo or 'Not assigned'}

TRANSCRIPT:
{transcript_truncated}

PARTNER BRANDS (brands we have revenue relationships with):
{', '.join(partner_names) if partner_names else 'No partner list provided'}

SIBLING VIDEO TRANSCRIPTS (other videos targeting same keyword):
{sibling_summaries if sibling_summaries else 'No sibling videos found'}

Evaluate these 4 gates:

1. PARTNER SAFETY: Does the transcript negatively reference any of the partner brands listed above? Look for negative sentiment, criticism, or disparagement near partner brand names. If no partner list is provided, PASS.

2. CROSS-VIDEO COHERENCE: Does this transcript contradict specific factual claims made in the sibling video transcripts above? Look for conflicting product recommendations, contradictory feature claims, or inconsistent pricing. If no siblings provided, PASS.

3. FUNNEL MATCH: Consider the target keyword. If it's a bottom-of-funnel (BOFU) keyword (e.g., "best X", "X review", "X vs Y", product-specific searches), does the script waste more than ~90 seconds on broad educational content that the viewer already understands? BOFU viewers want recommendations, not category education.

4. FACTUAL ACCURACY: List any claims in the transcript that reference specific numbers (prices, percentages, statistics, dates, feature counts) that could become outdated or incorrect. These need manual verification. If there are more than 3 unverifiable specific claims, FAIL.

Return ONLY valid JSON:
{{
    "partner_safety": {{
        "passed": true/false,
        "failure_reason": "specific reason or empty string"
    }},
    "cross_video_coherence": {{
        "passed": true/false,
        "failure_reason": "specific reason or empty string"
    }},
    "funnel_match": {{
        "passed": true/false,
        "failure_reason": "specific reason or empty string"
    }},
    "factual_accuracy": {{
        "passed": true/false,
        "failure_reason": "specific reason or empty string",
        "claims_to_verify": ["list of specific claims that need verification"]
    }}
}}"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = self._parse_json_response(message.content[0].text)
            data = json.loads(response_text)

            results = []
            gate_map = {
                'partner_safety': 'Partner Safety',
                'cross_video_coherence': 'Cross-Video Coherence',
                'funnel_match': 'Funnel Match',
                'factual_accuracy': 'Factual Accuracy',
            }
            for key, gate_name in gate_map.items():
                gate_data = data.get(key, {})
                results.append(GateCheckResult(
                    gate_name=gate_name,
                    passed=gate_data.get('passed', True),
                    failure_reason=gate_data.get('failure_reason', '')
                ))

            return results

        except Exception as e:
            logger.error(f"AI gate check failed: {str(e)}")
            # Return all passed on error (don't block on API failure)
            return [
                GateCheckResult(gate_name="Partner Safety", passed=True,
                               failure_reason="Gate check unavailable — API error"),
                GateCheckResult(gate_name="Cross-Video Coherence", passed=True,
                               failure_reason="Gate check unavailable — API error"),
                GateCheckResult(gate_name="Funnel Match", passed=True,
                               failure_reason="Gate check unavailable — API error"),
                GateCheckResult(gate_name="Factual Accuracy", passed=True,
                               failure_reason="Gate check unavailable — API error"),
            ]

    # ========== Layer 2: Quality Score ==========

    def score_quality(self, transcript: str, title: str, description: str,
                      duration_seconds: int, context: Dict) -> Optional[Dict]:
        """Score all 6 quality dimensions via single Claude API call.

        Returns dict with dimension scores, sub-scores, and action items.
        """
        transcript_truncated = transcript[:15000] if len(transcript) > 15000 else transcript
        description_truncated = description[:500] if len(description) > 500 else description
        keyword = context.get('main_keyword', '')
        silo = context.get('silo', '')

        # Pre-compute some deterministic signals to include in prompt context
        llm_phrases_found = [p for p in LLM_SMELL_PHRASES if p.lower() in transcript.lower()]
        vague_phrases_found = [p for p in VAGUE_CREDIBILITY_PHRASES if p.lower() in transcript.lower()]
        objection_phrases_found = [p for p in OBJECTION_PHRASES if p.lower() in transcript.lower()]

        prompt = f"""You are an expert YouTube script evaluator for an affiliate marketing channel.
Score this script across 6 weighted quality dimensions (100 points total).

TITLE: {title}
DESCRIPTION (first 500 chars): {description_truncated}
TARGET KEYWORD: {keyword or 'Unknown'}
SILO: {silo or 'Unknown'}
ESTIMATED DURATION: {duration_seconds}s ({duration_seconds // 60}m {duration_seconds % 60}s)

PRE-DETECTED SIGNALS:
- LLM smell phrases found: {json.dumps(llm_phrases_found) if llm_phrases_found else 'None'}
- Vague credibility phrases: {json.dumps(vague_phrases_found) if vague_phrases_found else 'None'}
- Objection-naming phrases: {json.dumps(objection_phrases_found) if objection_phrases_found else 'None'}

TRANSCRIPT:
{transcript_truncated}

Score each dimension with sub-scores. Be strict — most videos should score 50-70 total.
A score above 85 means genuinely exceptional across every dimension.

DIMENSION 1: SPECIFICITY & PROOF DENSITY (0-25 points)
Sub-scores (0-5 each):
- quantified_claims: Are credibility claims quantified ("47 hours" not "extensive testing")?
- feature_benefit_cascade: What % of features get translated to benefits → real scenarios?
- proof_density: Verifiable proof points per minute (target: 1 per 60s, first 30s get 2x weight)
- generic_content_ratio: What % of sentences could appear on ANY competitor review? Lower = better.
- llm_smell: Absence of AI-generated phrase patterns. 5 = fully human, 0 = reads like ChatGPT.

DIMENSION 2: CONVERSION ARCHITECTURE (0-20 points)
Sub-scores:
- cta_placement (0-5): At least 2 CTAs, first soft mention before 25% mark. Score timing as % of video.
- frame_control (0-5): Rate 1-5: 1=objections named & dismissed, 2=named & addressed, 3=embedded positive, 4=fit framing, 5=landscape definition. Map to 0-5 points.
- decisiveness (0-4): Closing 60 seconds — "This is the one" = high. "It depends" = low.
- risk_reversal (0-3): If product has guarantees/trials/returns, does script mention them?
- cognitive_leakage (0-3): Unnecessary competitor mentions that could send viewers away? 3=none, 0=many.

DIMENSION 3: RETENTION ARCHITECTURE (0-20 points)
Sub-scores:
- hook_specificity (0-5): First 30 seconds — concrete details vs vague promises? Numbers/comparisons = high.
- payoff_timing (0-4): If hook promises something, when does it deliver? Before 60% = good. After 70% = bad.
- chapter_quality (0-4): Chapter labels specific and curiosity-building? "Introduction" = deduction. Chapters <30s = flag.
- section_ordering (0-4): Does progression match viewer mental model? BOFU: recommendation before 40% mark.
- reveal_quality (0-3): Is the product reveal a decisive moment or does it slide by?

DIMENSION 4: AUTHENTICITY & NATURAL VOICE (0-15 points)
Sub-scores:
- personal_anecdote (0-4): At least one reference to personal experience that couldn't be scripted without using the product.
- personality_moments (0-4): Count humor, self-awareness, callbacks, fourth-wall breaks, distinctive voice.
- natural_language (0-3): Contractions, incomplete thoughts, colloquialisms, varied sentence length vs robotic perfection.
- llm_voice (0-2): Voice/tone markers — "Here's the thing" / "Without further ado" = LLM voice.
- survivability_2030 (0-2): Would this be distinguishable from AI content by 2030? 2=definitely human, 0=indistinguishable.

DIMENSION 5: VIEWER SOPHISTICATION RESPECT (0-10 points)
Sub-scores:
- cognitive_load (0-3): Translates technical specs into practical terms (breeds not measurements)?
- funnel_depth (0-3): Avoids re-explaining concepts the search intent proves viewer already knows?
- insider_knowledge (0-2): 1-2 moments of detail signaling domain expertise beyond basic research?
- reddit_skeptic (0-2): Doesn't pander to ultra-technical viewers, but throws them a bone?

DIMENSION 6: PRODUCTION STANDARDS (0-10 points, transcript-based proxy)
Sub-scores:
- broll_references (0-4): Does the script call for real product footage, screen recordings, dashboards?
- visual_evidence (0-3): When script makes comparative claims, does it reference showing data visually?
- screen_hygiene (0-3): Any references that might reveal cross-niche content? 3=clean, 0=risky.

Return ONLY valid JSON:
{{
    "specificity_proof_density": {{
        "total": <0-25>,
        "quantified_claims": <0-5>,
        "feature_benefit_cascade": <0-5>,
        "proof_density": <0-5>,
        "generic_content_ratio": <0-5>,
        "llm_smell": <0-5>,
        "examples": ["specific examples found..."],
        "deductions": ["specific issues..."]
    }},
    "conversion_architecture": {{
        "total": <0-20>,
        "cta_placement": <0-5>,
        "frame_control": <0-5>,
        "frame_control_level": <1-5>,
        "decisiveness": <0-4>,
        "risk_reversal": <0-3>,
        "cognitive_leakage": <0-3>,
        "cta_positions_pct": [<% positions of CTAs found>],
        "objection_phrases_found": [<phrases detected>]
    }},
    "retention_architecture": {{
        "total": <0-20>,
        "hook_specificity": <0-5>,
        "payoff_timing": <0-4>,
        "chapter_quality": <0-4>,
        "section_ordering": <0-4>,
        "reveal_quality": <0-3>,
        "hook_elements": ["what makes the hook work or not"],
        "payoff_delivery_pct": <% of runtime where payoff delivered>
    }},
    "authenticity_voice": {{
        "total": <0-15>,
        "personal_anecdote": <0-4>,
        "personality_moments": <0-4>,
        "natural_language": <0-3>,
        "llm_voice": <0-2>,
        "survivability_2030": <0-2>,
        "personality_moments_found": ["specific moments..."],
        "llm_phrases_flagged": ["specific phrases..."]
    }},
    "viewer_sophistication": {{
        "total": <0-10>,
        "cognitive_load": <0-3>,
        "funnel_depth": <0-3>,
        "insider_knowledge": <0-2>,
        "reddit_skeptic": <0-2>,
        "insider_moments": ["specific insider details found..."]
    }},
    "production_standards": {{
        "total": <0-10>,
        "broll_references": <0-4>,
        "visual_evidence": <0-3>,
        "screen_hygiene": <0-3>,
        "visual_references_found": ["specific references..."]
    }},
    "quality_score_total": <sum of all 6 dimension totals, should be 0-100>,
    "top_3_action_items": [
        {{
            "priority": 1,
            "dimension": "<dimension name>",
            "action": "<specific actionable fix>",
            "specific_detail": "<exact what to change/add, referencing the script content>"
        }},
        {{
            "priority": 2,
            "dimension": "<dimension name>",
            "action": "<specific actionable fix>",
            "specific_detail": "<exact what to change/add>"
        }},
        {{
            "priority": 3,
            "dimension": "<dimension name>",
            "action": "<specific actionable fix>",
            "specific_detail": "<exact what to change/add>"
        }}
    ]
}}"""

        try:
            logger.info(f"Scoring quality dimensions (transcript: {len(transcript)} chars)...")
            message = self.client.messages.create(
                model=self.model,
                max_tokens=3000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = self._parse_json_response(message.content[0].text)
            data = json.loads(response_text)

            # Extract dimension totals
            result = {
                'quality_score_total': data.get('quality_score_total', 0),
                'specificity_score': data.get('specificity_proof_density', {}).get('total', 0),
                'conversion_arch_score': data.get('conversion_architecture', {}).get('total', 0),
                'retention_arch_score': data.get('retention_architecture', {}).get('total', 0),
                'authenticity_score': data.get('authenticity_voice', {}).get('total', 0),
                'viewer_respect_score': data.get('viewer_sophistication', {}).get('total', 0),
                'production_score': data.get('production_standards', {}).get('total', 0),
                'dimension_details': {
                    'specificity_proof_density': data.get('specificity_proof_density', {}),
                    'conversion_architecture': data.get('conversion_architecture', {}),
                    'retention_architecture': data.get('retention_architecture', {}),
                    'authenticity_voice': data.get('authenticity_voice', {}),
                    'viewer_sophistication': data.get('viewer_sophistication', {}),
                    'production_standards': data.get('production_standards', {}),
                },
                'action_items': data.get('top_3_action_items', []),
            }

            # Validate total matches sum of dimensions
            expected_total = (
                result['specificity_score'] + result['conversion_arch_score'] +
                result['retention_arch_score'] + result['authenticity_score'] +
                result['viewer_respect_score'] + result['production_score']
            )
            if abs(result['quality_score_total'] - expected_total) > 1:
                logger.warning(f"Quality total mismatch: reported={result['quality_score_total']}, "
                              f"computed={expected_total}. Using computed value.")
                result['quality_score_total'] = round(expected_total, 1)

            logger.info(f"Quality score: {result['quality_score_total']}/100 "
                       f"(spec={result['specificity_score']}, conv={result['conversion_arch_score']}, "
                       f"ret={result['retention_arch_score']}, auth={result['authenticity_score']}, "
                       f"view={result['viewer_respect_score']}, prod={result['production_score']})")

            return result

        except Exception as e:
            logger.error(f"Quality scoring failed: {str(e)}")
            return None

    # ========== Layer 3: Context Multiplier ==========

    def compute_context_multiplier(self, video_id: str, context: Dict) -> Dict:
        """Compute context multiplier based on keyword tier and domination score.

        Uses the actual domination_score percentage from BigQuery (computed via
        the 3-CTE deduplication pattern in get_video_context), not rank-based proxies.

        Returns dict with keyword_tier, domination_score, multiplier, quality_floor.
        """
        silo = context.get('silo', '')
        latest_dom = context.get('latest_domination_score')
        avg_dom = context.get('avg_domination_score')

        # Determine keyword tier
        # Tier 1: silo has an approved brand (indicates priority keyword)
        # Tier 2: silo exists but no approved brand
        # AI/skeleton: no silo assigned
        approved = self.local_db.get_approved_brand_for_silo(silo) if silo else None

        if not silo:
            keyword_tier = 'ai_skeleton'
        elif approved:
            keyword_tier = 'tier1'
        else:
            keyword_tier = 'tier2'

        # Use latest domination score directly (already a percentage from BigQuery)
        domination_pct = latest_dom

        # Determine multiplier bucket
        if keyword_tier == 'ai_skeleton':
            bucket = 'ai_skeleton'
        elif keyword_tier == 'tier1':
            if domination_pct is not None and domination_pct >= 90:
                bucket = 'tier1_high_dom'
            else:
                bucket = 'tier1_low_dom'
        else:  # tier2
            # Check if proven revenue exists
            has_revenue = self._check_revenue_exists(video_id)
            if has_revenue:
                bucket = 'tier2_proven'
            else:
                bucket = 'tier2_unproven'

        multiplier, quality_floor = MULTIPLIER_TABLE[bucket]

        return {
            'keyword_tier': keyword_tier,
            'domination_score': domination_pct,
            'avg_domination_score': avg_dom,
            'multiplier': multiplier,
            'quality_floor': quality_floor,
            'bucket': bucket,
        }

    # ========== Layer 4: Rizz Score ==========

    # Filler word patterns (detected from transcript text, NOT from Hume emotions)
    FILLER_PATTERNS = [
        r'\bum\b', r'\buh\b', r'\blike\b(?=\s*,|\s*\.|\s+you)', r'\bkinda\b',
        r'\bsorta\b', r'\bi guess\b', r'\byou know\b', r'\bkind of\b',
        r'\bsort of\b', r'\bbasically\b',
    ]

    # Decisive language patterns
    DECISIVE_PHRASES = [
        r'\bthis is the one\b', r'\bhands down\b', r'\bi recommend\b',
        r'\bmy top pick\b', r'\bthe clear winner\b', r'\bwithout a doubt\b',
        r'\bdefinitely go with\b', r'\bby far the best\b', r'\byou need this\b',
        r'\byou should get\b', r'\bjust get\b', r'\bdon\'t hesitate\b',
        r'\bbest option\b', r'\bno question\b', r'\bgrab this\b',
        r'\bi\'d pick\b', r'\bi\'d go with\b', r'\bmy choice\b',
    ]

    # Hedging language patterns
    HEDGING_PHRASES = [
        r'\bit depends\b', r'\byou might\b', r'\bcould be\b', r'\bmaybe\b',
        r'\bperhaps\b', r'\bi\'m not sure\b', r'\bit\'s hard to say\b',
        r'\bsome people might\b', r'\bif you want\b', r'\bit\'s up to you\b',
        r'\bthere\'s no clear winner\b', r'\bboth are good\b',
    ]

    # First-person experience patterns
    FIRST_PERSON_EXPERIENCE = [
        r'\bi tested\b', r'\bi tried\b', r'\bin my experience\b',
        r'\bi found\b', r'\bi noticed\b', r'\bi\'ve been using\b',
        r'\bi used\b', r'\bpersonally\b', r'\bmy testing\b',
        r'\bi measured\b', r'\bi compared\b', r'\bi ran\b',
        r'\bi set up\b', r'\bi installed\b', r'\bafter using\b',
    ]

    # Generic product language patterns
    GENERIC_PRODUCT_PHRASES = [
        r'\bthis product features\b', r'\busers can expect\b',
        r'\bthe product offers\b', r'\bit comes with\b',
        r'\bthe manufacturer claims\b', r'\baccording to the specs\b',
        r'\bthe brand says\b', r'\bthe company states\b',
    ]

    def score_rizz(self, video_id: str, transcript: str,
                   emotions_data: Optional[Dict],
                   duration_seconds: int = 0) -> Dict:
        """Score presenter charisma: 60% vocal (Hume) + 40% copy (transcript).

        Args:
            video_id: YouTube video ID
            transcript: Full transcript text
            emotions_data: Hume AI emotion data (dict with 'segments' and 'summary')
            duration_seconds: Video duration in seconds

        Returns:
            Dict with rizz_score, rizz_vocal_score, rizz_copy_score, rizz_details
        """
        vocal_result = self._score_rizz_vocal(emotions_data, transcript, duration_seconds)
        copy_result = self._score_rizz_copy(video_id, transcript, duration_seconds)

        vocal_raw = vocal_result['total_raw']  # 0-100
        copy_raw = copy_result['total_raw']    # 0-100

        vocal_weighted = round(vocal_raw * 0.60, 1)  # 0-60
        copy_weighted = round(copy_raw * 0.40, 1)    # 0-40
        rizz_score = round(vocal_weighted + copy_weighted, 1)

        return {
            'rizz_score': rizz_score,
            'rizz_vocal_score': vocal_weighted,
            'rizz_copy_score': copy_weighted,
            'rizz_details': {
                'vocal': vocal_result,
                'copy': copy_result,
                'vocal_available': vocal_result.get('available', False),
            }
        }

    def _score_rizz_vocal(self, emotions_data: Optional[Dict],
                          transcript: str, duration_seconds: int) -> Dict:
        """Score vocal charisma from Hume AI emotion data (60% of Rizz).

        5 sub-metrics, each 0-20, total raw 0-100, then weighted x 0.60.
        """
        result = {
            'available': False,
            'total_raw': 0,
            'conviction_score': 0,
            'conviction_consistency': 0,
            'cta_conviction_delta': 0,
            'filler_density_score': 0,
            'pacing_variation_score': 0,
        }

        # Filler density and pacing can be scored from transcript even without Hume
        duration_minutes = max(duration_seconds / 60, 1) if duration_seconds else None

        # Sub-metric 4: Filler word density (from transcript text)
        filler_count = 0
        transcript_lower = transcript.lower()
        for pattern in self.FILLER_PATTERNS:
            filler_count += len(re.findall(pattern, transcript_lower))

        if duration_minutes:
            fillers_per_min = filler_count / duration_minutes
        else:
            # Estimate: ~150 words per minute
            word_count = len(transcript.split())
            est_minutes = max(word_count / 150, 1)
            fillers_per_min = filler_count / est_minutes

        # Sweet spot 1-3/min = 20, 0/min = 10 (suspicious), >5/min = 5
        if 1 <= fillers_per_min <= 3:
            filler_score = 20
        elif fillers_per_min < 1:
            filler_score = max(10, round(fillers_per_min * 10))  # 0 -> 0? no, PDF says 0/min = 10
            filler_score = 10  # suspicious scripted feel
        elif fillers_per_min <= 5:
            filler_score = round(20 - (fillers_per_min - 3) * 7.5)  # 3->20, 5->5
            filler_score = max(5, filler_score)
        else:
            filler_score = max(2, round(5 - (fillers_per_min - 5)))
        result['filler_density_score'] = min(20, max(0, filler_score))
        result['filler_count'] = filler_count
        result['fillers_per_min'] = round(fillers_per_min, 2)

        # Sub-metric 5: Pacing variation (from transcript segments or word-based estimation)
        pacing_score = self._compute_pacing_variation(transcript, duration_seconds)
        result['pacing_variation_score'] = pacing_score

        if not emotions_data or not emotions_data.get('segments'):
            # No Hume data — return transcript-only scores
            # Total raw = average of available metrics scaled to full range
            available_total = result['filler_density_score'] + result['pacing_variation_score']
            # Scale 2 metrics (max 40) to 0-100 range
            result['total_raw'] = round(available_total / 40 * 100, 1)
            result['note'] = 'Vocal analysis partial — no Hume emotion data available'
            return result

        # Hume data available
        result['available'] = True
        segments = emotions_data['segments']

        # Confidence emotions: Determination + Concentration + Interest
        CONFIDENCE_EMOTIONS = {'Determination', 'Concentration', 'Interest'}

        confidence_per_segment = []
        for seg in segments:
            top_emos = seg.get('top_emotions', [])
            conf_score = 0
            for emo in top_emos:
                if emo['emotion'] in CONFIDENCE_EMOTIONS:
                    conf_score += emo['score']
            confidence_per_segment.append(conf_score)

        if not confidence_per_segment:
            result['total_raw'] = round(
                (result['filler_density_score'] + result['pacing_variation_score']) / 40 * 100, 1
            )
            return result

        # Sub-metric 1: Vocal conviction score (0-20)
        avg_confidence = statistics.mean(confidence_per_segment)
        # Map avg score (0.0-0.5 range typically) to 0-20 points
        conviction_score = min(20, round(avg_confidence / 0.5 * 20))
        result['conviction_score'] = conviction_score
        result['avg_confidence'] = round(avg_confidence, 4)

        # Sub-metric 2: Conviction consistency (0-20)
        if len(confidence_per_segment) > 1:
            std_dev = statistics.stdev(confidence_per_segment)
            # Optimal std dev ~0.08-0.12 → 20 pts
            if 0.06 <= std_dev <= 0.14:
                consistency_score = 20
            elif std_dev < 0.06:
                # Too monotone
                consistency_score = round(std_dev / 0.06 * 20)
            else:
                # Too erratic
                consistency_score = max(0, round(20 - (std_dev - 0.14) * 100))
            consistency_score = min(20, max(0, consistency_score))
        else:
            consistency_score = 10  # Not enough data
        result['conviction_consistency'] = consistency_score
        result['confidence_std_dev'] = round(std_dev if len(confidence_per_segment) > 1 else 0, 4)

        # Sub-metric 3: CTA conviction delta (0-20)
        total_segs = len(confidence_per_segment)
        last_20pct_idx = max(1, int(total_segs * 0.8))
        overall_avg = avg_confidence
        cta_avg = statistics.mean(confidence_per_segment[last_20pct_idx:]) if confidence_per_segment[last_20pct_idx:] else overall_avg
        delta = cta_avg - overall_avg
        # Map 0.0-0.10 delta → 0-20 pts. Negative delta = bad
        if delta >= 0:
            cta_delta_score = min(20, round(delta / 0.10 * 20))
        else:
            cta_delta_score = max(0, round(10 + delta / 0.05 * 10))  # Slight penalty for dropping off
        result['cta_conviction_delta'] = cta_delta_score
        result['cta_delta'] = round(delta, 4)
        result['cta_avg_confidence'] = round(cta_avg, 4)

        # Total raw score (all 5 sub-metrics, each 0-20, total 0-100)
        result['total_raw'] = min(100, (
            result['conviction_score'] +
            result['conviction_consistency'] +
            result['cta_conviction_delta'] +
            result['filler_density_score'] +
            result['pacing_variation_score']
        ))

        return result

    def _compute_pacing_variation(self, transcript: str, duration_seconds: int) -> int:
        """Compute pacing variation score (0-20) from transcript word distribution."""
        # Split transcript into ~10 equal chunks
        words = transcript.split()
        if len(words) < 50:
            return 10  # Too short to evaluate

        chunk_size = max(len(words) // 10, 10)
        chunks = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]
        if len(chunks) < 3:
            return 10

        # Words per chunk (proxy for words-per-minute variation)
        wpcs = [len(c) for c in chunks]
        mean_wpc = statistics.mean(wpcs)
        if mean_wpc == 0:
            return 10

        cv = statistics.stdev(wpcs) / mean_wpc if len(wpcs) > 1 else 0

        # CV 0.15-0.35 = ideal (20 pts)
        if 0.15 <= cv <= 0.35:
            return 20
        elif cv < 0.15:
            return max(5, round(cv / 0.15 * 20))
        else:
            return max(5, round(20 - (cv - 0.35) * 30))

    def _score_rizz_copy(self, video_id: str, transcript: str,
                         duration_seconds: int) -> Dict:
        """Score copy charisma from transcript text (40% of Rizz).

        4 sub-metrics, each 0-25, total raw 0-100, then weighted x 0.40.
        """
        result = {
            'total_raw': 0,
            'personality_density': 0,
            'sentence_variation': 0,
            'decisive_language': 0,
            'first_person_experience': 0,
        }

        duration_minutes = max(duration_seconds / 60, 1) if duration_seconds else None
        if not duration_minutes:
            word_count = len(transcript.split())
            duration_minutes = max(word_count / 150, 1)

        # Sub-metric 1: Personality density (requires 1 Claude call)
        personality_result = self._score_personality_density(transcript, duration_minutes)
        result['personality_density'] = personality_result['score']
        result['personality_moments_found'] = personality_result.get('moments', [])
        result['personality_per_min'] = personality_result.get('per_minute', 0)

        # Sub-metric 2: Sentence length variation (deterministic)
        sentences = re.split(r'[.!?]+', transcript)
        sentences = [s.strip() for s in sentences if s.strip() and len(s.split()) >= 2]
        if len(sentences) >= 5:
            word_counts = [len(s.split()) for s in sentences]
            mean_wc = statistics.mean(word_counts)
            if mean_wc > 0 and len(word_counts) > 1:
                cv = statistics.stdev(word_counts) / mean_wc
                # CV 0.4-0.7 = ideal (25 pts). <0.2 = robotic (5 pts)
                if 0.4 <= cv <= 0.7:
                    sent_score = 25
                elif cv < 0.2:
                    sent_score = 5
                elif cv < 0.4:
                    sent_score = round(5 + (cv - 0.2) / 0.2 * 20)
                else:
                    sent_score = max(10, round(25 - (cv - 0.7) * 25))
                result['sentence_variation'] = min(25, max(0, sent_score))
                result['sentence_cv'] = round(cv, 3)
            else:
                result['sentence_variation'] = 10
        else:
            result['sentence_variation'] = 10

        # Sub-metric 3: Decisive language ratio (deterministic)
        transcript_lower = transcript.lower()
        decisive_count = sum(
            len(re.findall(p, transcript_lower)) for p in self.DECISIVE_PHRASES
        )
        hedging_count = sum(
            len(re.findall(p, transcript_lower)) for p in self.HEDGING_PHRASES
        )
        # Ratio >= 3:1 = 25 pts
        if hedging_count == 0:
            ratio = decisive_count if decisive_count > 0 else 0
        else:
            ratio = decisive_count / hedging_count

        if ratio >= 3:
            decisive_score = 25
        elif ratio >= 2:
            decisive_score = 20
        elif ratio >= 1:
            decisive_score = 15
        elif ratio >= 0.5:
            decisive_score = 10
        else:
            decisive_score = max(3, round(ratio * 20))
        result['decisive_language'] = min(25, decisive_score)
        result['decisive_count'] = decisive_count
        result['hedging_count'] = hedging_count
        result['decisive_ratio'] = round(ratio, 2)

        # Sub-metric 4: First-person experience ratio (deterministic)
        first_person_count = sum(
            len(re.findall(p, transcript_lower)) for p in self.FIRST_PERSON_EXPERIENCE
        )
        generic_count = sum(
            len(re.findall(p, transcript_lower)) for p in self.GENERIC_PRODUCT_PHRASES
        )
        if generic_count == 0:
            fp_ratio = first_person_count if first_person_count > 0 else 0
        else:
            fp_ratio = first_person_count / generic_count

        # Ratio >= 2:1 = 25 pts
        if fp_ratio >= 2:
            fp_score = 25
        elif fp_ratio >= 1.5:
            fp_score = 20
        elif fp_ratio >= 1:
            fp_score = 15
        elif fp_ratio >= 0.5:
            fp_score = 10
        else:
            fp_score = max(3, round(fp_ratio * 20))
        result['first_person_experience'] = min(25, fp_score)
        result['first_person_count'] = first_person_count
        result['generic_count'] = generic_count
        result['first_person_ratio'] = round(fp_ratio, 2)

        # Total raw score (4 sub-metrics, each 0-25, total 0-100)
        result['total_raw'] = min(100, (
            result['personality_density'] +
            result['sentence_variation'] +
            result['decisive_language'] +
            result['first_person_experience']
        ))

        return result

    def _score_personality_density(self, transcript: str, duration_minutes: float) -> Dict:
        """Score personality density via 1 Claude call — humor, callbacks, metaphors, etc.

        Target: >= 1 personality moment per 2 minutes = 25 pts.
        """
        transcript_truncated = transcript[:10000] if len(transcript) > 10000 else transcript

        prompt = f"""You are analyzing a YouTube video transcript for personality moments.

Count these types of personality moments in the transcript:
- Humor (jokes, wit, sarcasm, self-deprecation)
- Callbacks (references to earlier points or running gags)
- Self-awareness / fourth-wall breaks (addressing the viewer directly about the format)
- Vivid metaphors or analogies
- Personal anecdotes that feel genuine
- Catchphrases or signature expressions

TRANSCRIPT:
{transcript_truncated}

Return ONLY valid JSON:
{{
    "moments": [
        {{"type": "<type>", "quote": "<short exact quote from transcript>", "timestamp_hint": "<rough position: beginning/middle/end>"}}
    ],
    "total_count": <number>
}}"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}]
            )
            response_text = self._parse_json_response(message.content[0].text)
            data = json.loads(response_text)

            total = data.get('total_count', 0)
            per_minute = total / duration_minutes if duration_minutes > 0 else 0

            # Target: >= 1 per 2 minutes (0.5/min) = 25 pts
            if per_minute >= 0.5:
                score = 25
            elif per_minute >= 0.3:
                score = 20
            elif per_minute >= 0.2:
                score = 15
            elif per_minute >= 0.1:
                score = 10
            else:
                score = max(3, round(per_minute / 0.1 * 10))

            return {
                'score': min(25, score),
                'moments': data.get('moments', [])[:5],
                'total_count': total,
                'per_minute': round(per_minute, 2),
            }

        except Exception as e:
            logger.error(f"Personality density scoring failed: {str(e)}")
            return {'score': 10, 'moments': [], 'total_count': 0, 'per_minute': 0}

    # ========== Helpers ==========

    def _get_video_context(self, video_id: str) -> Dict:
        """Get video context from BigQuery."""
        try:
            return self.bigquery.get_video_context(video_id)
        except Exception as e:
            logger.error(f"Error getting video context: {str(e)}")
            return {}

    def _check_revenue_exists(self, video_id: str) -> bool:
        """Check if video has any revenue history in BigQuery."""
        try:
            perf = self.bigquery.get_affiliate_performance(video_id)
            return bool(perf and any(p.get('total_revenue', 0) > 0 for p in perf))
        except Exception:
            return False

    def _parse_json_response(self, text: str) -> str:
        """Strip markdown code blocks from Claude response."""
        text = text.strip()
        if text.startswith('```'):
            text = text.split('\n', 1)[1] if '\n' in text else text[3:]
            if text.endswith('```'):
                text = text.rsplit('```', 1)[0]
            text = text.strip()
        return text
