"""
SOK MetaManager — Transliteration & Language Module
Handles Indian language detection, romanization scheme handling,
auto-copy of English→alt_ fields, and script conversion.
"""
import re
from typing import Optional, Tuple, Dict, List

# ── Language normalisation ────────────────────────────────────────────────────
# IA language fields are wildly inconsistent: "Kannada", "kan", "kn", "KAN" …
LANGUAGE_NORM: Dict[str, str] = {
    # Kannada
    'kan': 'kan', 'kannada': 'kan', 'kn': 'kan', 'ಕನ್ನಡ': 'kan',
    # Telugu
    'tel': 'tel', 'telugu': 'tel', 'te': 'tel', 'తెలుగు': 'tel',
    # Tamil
    'tam': 'tam', 'tamil': 'tam', 'ta': 'tam', 'தமிழ்': 'tam',
    # Malayalam
    'mal': 'mal', 'malayalam': 'mal', 'ml': 'mal', 'മലയാളം': 'mal',
    # Hindi
    'hin': 'hin', 'hindi': 'hin', 'hi': 'hin', 'हिन्दी': 'hin',
    'hindi language': 'hin',
    # Sanskrit
    'san': 'san', 'sanskrit': 'san', 'sa': 'san', 'संस्कृत': 'san',
    'samskrita': 'san', 'samskritam': 'san',
    # Marathi
    'mar': 'mar', 'marathi': 'mar', 'mr': 'mar', 'मराठी': 'mar',
    # Bengali
    'ben': 'ben', 'bengali': 'ben', 'bangla': 'ben', 'bn': 'ben', 'বাংলা': 'ben',
    # Gujarati
    'guj': 'guj', 'gujarati': 'guj', 'gu': 'guj', 'ગુજરાતી': 'guj',
    # Punjabi
    'pan': 'pan', 'punjabi': 'pan', 'panjabi': 'pan', 'pa': 'pan', 'ਪੰਜਾਬੀ': 'pan',
    # Odia
    'ori': 'ori', 'odia': 'ori', 'oriya': 'ori', 'or': 'ori', 'ଓଡ଼ିଆ': 'ori',
    # Urdu
    'urd': 'urd', 'urdu': 'urd', 'ur': 'urd', 'اردو': 'urd',
    # Assamese
    'asm': 'asm', 'assamese': 'asm', 'as': 'asm',
    # Konkani
    'kok': 'kok', 'konkani': 'kok',
    # Tulu
    'tcy': 'tcy', 'tulu': 'tcy',
    # English
    'eng': 'eng', 'english': 'eng', 'en': 'eng',
}

LANGUAGE_LABELS: Dict[str, str] = {
    'kan': 'Kannada', 'tel': 'Telugu', 'tam': 'Tamil',
    'mal': 'Malayalam', 'hin': 'Hindi', 'san': 'Sanskrit',
    'mar': 'Marathi', 'ben': 'Bengali', 'guj': 'Gujarati',
    'pan': 'Punjabi', 'ori': 'Odia', 'urd': 'Urdu',
    'asm': 'Assamese', 'kok': 'Konkani', 'tcy': 'Tulu',
    'eng': 'English',
}

# Language codes that are Indian (non-English) and need transliteration
INDIAN_CODES = {
    'kan', 'tel', 'tam', 'mal', 'hin', 'san', 'mar',
    'ben', 'guj', 'pan', 'ori', 'urd', 'asm', 'kok', 'tcy',
}

# Fields that are candidates for auto-copy English → alt_ and transliteration
TRANSLIT_FIELDS = ['title', 'creator', 'author', 'publisher']
ALT_FIELD_MAP   = {
    'title':     'alt_title',
    'creator':   'alt_creator',
    'author':    'alt_author',
    'publisher': 'alt_publisher',
}

# Supported romanization input schemes (display label → library key)
INPUT_SCHEMES = {
    'ITRANS':    'itrans',   # Most common casual Indian romanization
    'HK':        'hk',       # Harvard-Kyoto (scholarly)
    'IAST':      'iast',     # International Alphabet of Sanskrit Transliteration
    'SLP1':      'slp1',     # Sanskrit Library Phonetic Basic
    'Velthuis':  'velthuis',
    'WX':        'wx',
}


def normalize_language(raw: str) -> Optional[str]:
    """Return a normalised 3-letter language code or None."""
    if not raw:
        return None
    key = raw.strip().lower()
    return LANGUAGE_NORM.get(key)


def is_indian(lang_code: Optional[str]) -> bool:
    return bool(lang_code and lang_code in INDIAN_CODES)


def get_language_label(code: str) -> str:
    return LANGUAGE_LABELS.get(code, code.upper())


# ── Script mapping ────────────────────────────────────────────────────────────

def _get_script(lang_code: str):
    """Return the indic_transliteration script constant for a language."""
    try:
        from indic_transliteration import sanscript as S
        return {
            'kan': S.KANNADA,
            'tel': S.TELUGU,
            'tam': S.TAMIL,
            'mal': S.MALAYALAM,
            'hin': S.DEVANAGARI,
            'san': S.DEVANAGARI,
            'mar': S.DEVANAGARI,
            'ben': S.BENGALI,
            'guj': S.GUJARATI,
            'pan': S.GURMUKHI,
            'ori': S.ORIYA,
            'asm': S.BENGALI,   # Assamese uses Bengali script
        }.get(lang_code)
    except ImportError:
        return None


def _get_source_scheme(scheme_key: str):
    """Return the indic_transliteration scheme constant."""
    try:
        from indic_transliteration import sanscript as S
        return {
            'itrans':   S.ITRANS,
            'hk':       S.HK,
            'iast':     S.IAST,
            'slp1':     S.SLP1,
            'velthuis': S.VELTHUIS,
            'wx':       S.WX,
        }.get(scheme_key, S.ITRANS)
    except ImportError:
        return None


# ── Transliteration ───────────────────────────────────────────────────────────

def transliterate_text(
    text: str,
    target_lang: str,
    source_scheme: str = 'itrans'
) -> Tuple[str, float]:
    """
    Transliterate romanized text to the target Indian script.
    Returns (result_text, confidence 0.0-1.0).

    Confidence reflects how well the source text fits the chosen scheme:
    - IAST/HK with diacritic markers → high confidence (~0.85)
    - Clean ITRANS patterns → moderate (~0.60)
    - Plain casual English → lower (~0.35) — human review needed
    """
    if not text or not target_lang:
        return text or '', 0.0

    try:
        from indic_transliteration.sanscript import transliterate
        target_script = _get_script(target_lang)
        source        = _get_source_scheme(source_scheme)
        if not target_script or not source:
            return text, 0.0

        result = transliterate(text.strip(), source, target_script)

        # Estimate confidence
        diacritics = len(re.findall(r'[āīūṛṝḷṃḥṅñṭḍṇśṣÃÄÅÇÈÉ]', text))
        itrans_marks = len(re.findall(r'[AEIOU]{2}|aa|ii|uu|sh|jn|kh|gh|ch', text))
        if diacritics >= 2:
            conf = 0.85
        elif itrans_marks >= 2:
            conf = 0.60
        else:
            conf = 0.35     # Casual English — must be reviewed

        return result, round(conf, 2)

    except ImportError:
        return text, 0.0
    except Exception:
        return text, 0.0


def check_lib_available() -> bool:
    try:
        import indic_transliteration  # noqa
        return True
    except ImportError:
        return False


# ── Auto-copy English → alt_ ──────────────────────────────────────────────────

def build_alt_copy(item: dict) -> dict:
    """
    For a non-English item: copy each main field value → its alt_ counterpart
    ONLY if alt_ field is currently empty.
    Returns a dict of {alt_field: value} to be applied.
    """
    updates = {}
    for src, dst in ALT_FIELD_MAP.items():
        src_val = (item.get(src) or '').strip()
        dst_val = (item.get(dst) or '').strip()
        if src_val and not dst_val:
            updates[dst] = src_val
    return updates


# ── Batch helpers ─────────────────────────────────────────────────────────────

def fields_needing_translit(item: dict) -> List[str]:
    """Return list of fields that have a value but no transliteration yet."""
    needs = []
    for field in TRANSLIT_FIELDS:
        val = (item.get(field) or '').strip()
        if val:
            needs.append(field)
    return needs


def language_summary(items: List[dict]) -> List[dict]:
    """
    Aggregate items by detected_language.
    Returns list of {code, label, total, needs_copy, needs_translit, reviewed}.
    """
    buckets: Dict[str, dict] = {}
    for item in items:
        code = item.get('detected_language') or 'unknown'
        if code not in buckets:
            buckets[code] = {
                'code': code,
                'label': get_language_label(code) if code != 'unknown' else 'Unknown / Unset',
                'total': 0, 'needs_copy': 0,
                'needs_translit': 0, 'reviewed': 0,
            }
        b = buckets[code]
        b['total'] += 1
        ts = item.get('translit_status', 'none')
        if ts == 'none':
            b['needs_copy'] += 1
        elif ts == 'copied':
            b['needs_translit'] += 1
        elif ts in ('generated', 'reviewed', 'finalized'):
            b['reviewed'] += 1
    return sorted(buckets.values(), key=lambda x: -x['total'])
