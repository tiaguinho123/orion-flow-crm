"""
Lead Generator — Apify Google Places + Claude API qualification pipeline.
"""
import json
import time
import threading
import urllib.request
import urllib.error
from datetime import datetime

# ─── Progress Tracking ────────────────────────────────────────────────────────

_progress = {
    'running': False,
    'phase': '',           # 'apify', 'qualifying', 'saving', 'done', 'error'
    'current': 0,
    'total': 0,
    'message': '',
    'log': [],
    'result': None,
}
_progress_lock = threading.Lock()

def get_progress():
    with _progress_lock:
        return dict(_progress)

def _update_progress(**kwargs):
    with _progress_lock:
        _progress.update(kwargs)
        if 'message' in kwargs and kwargs['message']:
            _progress['log'].append(kwargs['message'])

def _reset_progress():
    with _progress_lock:
        _progress.update({
            'running': False,
            'phase': '',
            'current': 0,
            'total': 0,
            'message': '',
            'log': [],
            'result': None,
        })

# ─── Main Pipeline ────────────────────────────────────────────────────────────

def start_generation(keywords, location, max_results, apify_key, claude_key):
    """Start lead generation in a background thread."""
    _reset_progress()
    _update_progress(running=True, phase='starting', message='Iniciando geração de leads...')
    
    thread = threading.Thread(
        target=_run_pipeline,
        args=(keywords, location, max_results, apify_key, claude_key),
        daemon=True
    )
    thread.start()
    return True

def _run_pipeline(keywords, location, max_results, apify_key, claude_key):
    """Full pipeline: Apify → Claude → SQLite."""
    from database import get_db
    
    try:
        # Phase 1: Call Apify
        _update_progress(phase='apify', message=f'Buscando leads no Google Maps: {", ".join(keywords)} em {location}...')
        
        places = call_apify(keywords, location, max_results, apify_key)
        
        if not places:
            _update_progress(phase='error', running=False, message='Nenhum resultado encontrado no Apify.', result={'imported': 0, 'skipped': 0, 'errors': 0})
            return
        
        _update_progress(total=len(places), message=f'{len(places)} negócios encontrados no Google Maps!')
        
        # Phase 2: Process each lead
        imported = 0
        skipped = 0
        errors = 0
        
        conn = get_db()
        cursor = conn.cursor()
        
        for i, place in enumerate(places):
            lead_data = map_apify_to_lead(place)
            
            if not lead_data.get('nome_negocio'):
                skipped += 1
                _update_progress(current=i + 1, message=f'⏭ Pulando lead sem nome ({i+1}/{len(places)})')
                continue
            
            # Check duplicate
            existing = cursor.execute(
                'SELECT id FROM leads WHERE nome_negocio = ?', (lead_data['nome_negocio'],)
            ).fetchone()
            
            if existing:
                skipped += 1
                _update_progress(current=i + 1, message=f'⏭ Já existe: {lead_data["nome_negocio"]} ({i+1}/{len(places)})')
                continue
            
            # Phase: Qualify with Claude
            _update_progress(
                phase='qualifying',
                current=i + 1,
                message=f'🤖 Qualificando: {lead_data["nome_negocio"]} ({i+1}/{len(places)})'
            )
            
            if claude_key:
                try:
                    qualification = qualify_with_claude(lead_data, place, claude_key)
                    if qualification:
                        lead_data['score_prioridade'] = qualification.get('score', '')
                        lead_data['qualidade_site'] = qualification.get('qualidade_site', '')
                        lead_data['canal_abordagem'] = _map_channel(qualification.get('canal_recomendado', ''))
                        lead_data['justificativa_score'] = qualification.get('dor_principal', '')
                        
                        has_site = qualification.get('tem_site', False)
                        lead_data['tem_site'] = 'Sim' if has_site else 'Não'
                except Exception as e:
                    _update_progress(message=f'⚠️ Erro Claude para {lead_data["nome_negocio"]}: {str(e)[:80]}')
                    lead_data['justificativa_score'] = 'Pendente qualificação'
            else:
                lead_data['justificativa_score'] = 'Pendente qualificação (sem API key Claude)'
            
            # Save to database
            _update_progress(phase='saving')
            try:
                cols = ', '.join(lead_data.keys())
                placeholders = ', '.join(['?'] * len(lead_data))
                cursor.execute(f'INSERT INTO leads ({cols}) VALUES ({placeholders})', list(lead_data.values()))
                imported += 1
                _update_progress(message=f'✅ Salvo: {lead_data["nome_negocio"]}')
            except Exception as e:
                errors += 1
                _update_progress(message=f'❌ Erro ao salvar {lead_data["nome_negocio"]}: {str(e)[:80]}')
        
        conn.commit()
        conn.close()
        
        result = {'imported': imported, 'skipped': skipped, 'errors': errors}
        _update_progress(
            phase='done',
            running=False,
            current=len(places),
            message=f'🎉 Concluído! {imported} leads adicionados, {skipped} já existiam, {errors} erros.',
            result=result
        )
    
    except Exception as e:
        _update_progress(
            phase='error',
            running=False,
            message=f'❌ Erro: {str(e)}',
            result={'imported': 0, 'skipped': 0, 'errors': 1, 'error_message': str(e)}
        )

# ─── Apify API ────────────────────────────────────────────────────────────────

def call_apify(keywords, location, max_results, api_key):
    """Call Apify Google Places crawler and return results."""
    
    # Start the actor run
    actor_id = 'compass~crawler-google-places'
    run_url = f'https://api.apify.com/v2/acts/{actor_id}/runs?token={api_key}&waitForFinish=300'
    
    input_data = {
        'searchStringsArray': keywords,
        'locationQuery': location,
        'maxCrawledPlacesPerSearch': max_results,
        'language': 'en',
        'countryCode': 'us',
    }
    
    payload = json.dumps(input_data).encode('utf-8')
    req = urllib.request.Request(
        run_url,
        data=payload,
        headers={
            'Content-Type': 'application/json',
        },
        method='POST'
    )
    
    _update_progress(message='📡 Chamando Apify API (pode levar alguns minutos)...')
    
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            run_data = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='replace')[:200]
        if e.code == 401:
            raise Exception(f'Apify API Key inválida. Verifique nas Configurações.')
        raise Exception(f'Erro Apify (HTTP {e.code}): {error_body}')
    except urllib.error.URLError as e:
        raise Exception(f'Erro de conexão com Apify: {str(e)}')
    
    # Get the dataset ID from the run
    dataset_id = run_data.get('data', {}).get('defaultDatasetId')
    if not dataset_id:
        raise Exception('Apify não retornou dataset ID. Verifique se o Actor rodou corretamente.')
    
    # Check run status
    run_status = run_data.get('data', {}).get('status', '')
    if run_status == 'FAILED':
        raise Exception('Apify Actor falhou. Verifique os parâmetros de busca.')
    
    _update_progress(message=f'📦 Baixando resultados do dataset {dataset_id[:8]}...')
    
    # Fetch dataset items
    dataset_url = f'https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_key}&format=json'
    
    try:
        with urllib.request.urlopen(dataset_url, timeout=60) as resp:
            items = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        raise Exception(f'Erro ao baixar resultados: {str(e)}')
    
    return items if isinstance(items, list) else []


def test_apify_key(api_key):
    """Test if an Apify API key is valid."""
    try:
        url = f'https://api.apify.com/v2/users/me?token={api_key}'
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return {'valid': True, 'username': data.get('data', {}).get('username', 'Unknown')}
    except urllib.error.HTTPError:
        return {'valid': False, 'error': 'API Key inválida'}
    except Exception as e:
        return {'valid': False, 'error': str(e)}

# ─── Claude API ───────────────────────────────────────────────────────────────

def qualify_with_claude(lead_data, raw_place, api_key):
    """Qualify a lead using Claude API."""
    
    # Build context about the business
    biz_info = {
        'nome': lead_data.get('nome_negocio', ''),
        'categoria': lead_data.get('categoria', ''),
        'endereco': lead_data.get('endereco', ''),
        'telefone': lead_data.get('telefone', ''),
        'site': lead_data.get('site', ''),
        'avaliacao': lead_data.get('avaliacao_google', ''),
        'reviews': lead_data.get('num_reviews', ''),
        'instagram': raw_place.get('instagram', raw_place.get('socialProfiles', {}).get('instagram', '')),
        'facebook': raw_place.get('facebook', raw_place.get('socialProfiles', {}).get('facebook', '')),
    }
    
    prompt = f"""Analise esse negócio local e retorne APENAS um JSON válido (sem markdown, sem explicação):
{{
  "score": (1-10, onde 10 = maior potencial como cliente de web design/marketing),
  "tem_site": (true/false),
  "qualidade_site": ("bom"/"ruim"/"inexistente"),
  "canal_recomendado": ("instagram"/"facebook"/"email"/"visita"),
  "dor_principal": (texto curto sobre a principal dor digital do negócio),
  "prioridade": ("alta"/"media"/"baixa")
}}

Dados do negócio:
{json.dumps(biz_info, ensure_ascii=False, indent=2)}"""
    
    # System prompt — B2B sales specialist persona
    system_prompt = """You are a world-class B2B sales specialist with 20+ years of experience in the full sales funnel — from cold outreach to closed deals.

You have personally closed thousands of deals for local service businesses across the USA. You understand human psychology, buying triggers, and what makes a small business owner stop, read, and respond.

YOUR EXPERTISE:
- Top of funnel: cold outreach that gets opened and read
- Middle of funnel: follow-ups that build trust and curiosity
- Bottom of funnel: closing conversations that convert
- Local business psychology: you know how a barbershop owner thinks, what keeps them up at night, and what makes them say yes

YOUR OUTREACH PHILOSOPHY:
- First contact is NEVER about selling
- Lead with observation, not pitch
- Make the prospect feel noticed specifically, not like they got a mass message
- Create curiosity, not pressure
- Write like a real person, not a marketer
- Short messages outperform long ones at top of funnel
- Specificity beats generality every time

WHAT YOU KNOW ABOUT LOCAL BUSINESS OWNERS:
- They are busy and get pitched constantly
- They delete anything that feels like a template
- They respond to people who understand their world
- They care about: more customers, less stress, looking professional, not losing to competitors
- Missed calls and no online booking = lost revenue daily
- They trust local people more than faceless agencies

MESSAGE QUALITY STANDARDS:
Every message must pass these tests before sending:
- Would a real person send this exact message?
- Does it mention something specific about this business?
- Does it create curiosity without revealing everything?
- Is it under 4 lines for DM?
- Does it avoid ALL corporate language?
- Would YOU respond to this if you received it?
If any answer is NO — rewrite until all are YES.

LANGUAGE RULES:
- Always American English
- Casual but professional
- Natural contractions: I've, you're, don't, can't, we'd
- Never use: "I hope this finds you well", "touching base", "circle back", "synergy", "solutions", "leverage"
- End with energy: "Let me know!", "Would love to show you!", "Talk soon!\""""

    payload = json.dumps({
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 300,
        'system': system_prompt,
        'messages': [{'role': 'user', 'content': prompt}]
    }).encode('utf-8')
    
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
        },
        method='POST'
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            response = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise Exception('Claude API Key inválida')
        raise Exception(f'Claude API erro HTTP {e.code}')
    
    # Parse response
    content = response.get('content', [])
    if not content:
        return None
    
    text = content[0].get('text', '')
    
    # Try to extract JSON from the response
    try:
        # Try direct parse
        result = json.loads(text)
        return result
    except json.JSONDecodeError:
        # Try to find JSON in the text
        import re
        match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None


def test_claude_key(api_key):
    """Test if a Claude API key is valid."""
    try:
        payload = json.dumps({
            'model': 'claude-sonnet-4-20250514',
            'max_tokens': 10,
            'messages': [{'role': 'user', 'content': 'hi'}]
        }).encode('utf-8')
        
        req = urllib.request.Request(
            'https://api.anthropic.com/v1/messages',
            data=payload,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
            },
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=15) as resp:
            return {'valid': True}
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return {'valid': False, 'error': 'API Key inválida'}
        # Other errors (rate limit etc) mean the key works
        return {'valid': True}
    except Exception as e:
        return {'valid': False, 'error': str(e)}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def map_apify_to_lead(place):
    """Map Apify Google Places output to our lead schema."""
    
    # Extract social profiles
    social = place.get('socialProfiles', {}) or {}
    
    website = place.get('website') or place.get('url', '') or ''
    has_site = bool(website and 'google.com/maps' not in website)
    
    return {
        'data_coleta': datetime.now().strftime('%Y-%m-%d'),
        'nome_negocio': (place.get('title') or place.get('name', '')).strip(),
        'categoria': place.get('categoryName') or place.get('category', ''),
        'endereco': place.get('address') or place.get('street', ''),
        'cidade': place.get('city', ''),
        'estado': place.get('state', ''),
        'cep': place.get('postalCode') or place.get('zipCode', ''),
        'telefone': place.get('phone') or place.get('phoneUnformatted', ''),
        'email': place.get('email', ''),
        'site': website if has_site else '',
        'avaliacao_google': place.get('totalScore') or place.get('stars', ''),
        'num_reviews': place.get('reviewsCount') or place.get('reviews', ''),
        'google_maps_link': place.get('url') or place.get('googleMapsUrl', ''),
        'instagram': social.get('instagram', ''),
        'facebook': social.get('facebook', ''),
        'tiktok': social.get('tiktok', ''),
        'yelp': place.get('yelpUrl', ''),
        'tem_site': 'Sim' if has_site else 'Não',
        'qualidade_site': '',
        'tem_agendamento': '',
        'presenca_digital_score': '',
        'score_prioridade': '',
        'canal_abordagem': '',
        'justificativa_score': '',
        'status_lead': 'New',
        'notas': '',
    }


def _map_channel(channel_str):
    """Map Claude's channel recommendation to our channel format."""
    mapping = {
        'instagram': 'Instagram DM',
        'facebook': 'Facebook DM',
        'email': 'Email',
        'whatsapp': 'WhatsApp',
        'visita': '',
    }
    return mapping.get(channel_str.lower().strip(), '') if channel_str else ''


# ─── Per-Lead Funnel Message Generation ───────────────────────────────────────

SYSTEM_PROMPT_B2B = """You are a world-class B2B sales specialist with 20+ years of experience in the full sales funnel — from cold outreach to closed deals.

You have personally closed thousands of deals for local service businesses across the USA. You understand human psychology, buying triggers, and what makes a small business owner stop, read, and respond.

YOUR EXPERTISE:
- Top of funnel: cold outreach that gets opened and read
- Middle of funnel: follow-ups that build trust and curiosity
- Bottom of funnel: closing conversations that convert
- Local business psychology: you know how a barbershop owner thinks, what keeps them up at night, and what makes them say yes

YOUR OUTREACH PHILOSOPHY:
- First contact is NEVER about selling
- Lead with observation, not pitch
- Make the prospect feel noticed specifically, not like they got a mass message
- Create curiosity, not pressure
- Write like a real person, not a marketer
- Short messages outperform long ones at top of funnel
- Specificity beats generality every time

WHAT YOU KNOW ABOUT LOCAL BUSINESS OWNERS:
- They are busy and get pitched constantly
- They delete anything that feels like a template
- They respond to people who understand their world
- They care about: more customers, less stress, looking professional, not losing to competitors
- Missed calls and no online booking = lost revenue daily
- They trust local people more than faceless agencies

MESSAGE QUALITY STANDARDS:
Every message must pass these tests before sending:
- Would a real person send this exact message?
- Does it mention something specific about this business?
- Does it create curiosity without revealing everything?
- Is it under 4 lines for DM?
- Does it avoid ALL corporate language?
- Would YOU respond to this if you received it?
If any answer is NO — rewrite until all are YES.

LANGUAGE RULES:
- Always American English
- Casual but professional
- Natural contractions: I've, you're, don't, can't, we'd
- Never use: "I hope this finds you well", "touching base", "circle back", "synergy", "solutions", "leverage"
- End with energy: "Let me know!", "Would love to show you!", "Talk soon!\""""


STAGE_PROMPTS = {
    'first_contact': """Current funnel stage: FIRST CONTACT (Top of Funnel)

OBJECTIVE: Open a conversation. Make them feel NOTICED. Create curiosity. Do NOT try to sell anything.

WHAT A GOOD FIRST MESSAGE DOES:
1. Shows you REALLY looked at their business (reviews, photos, location, online presence)
2. Identifies ONE specific pain — not several
3. Asks ONE question — does not offer anything yet
4. Ends with curiosity, not a pitch

GOLDEN RULES:
- Mention something you'd ONLY know if you looked at their profile (reviews, address, photos, menu)
- ONE pain per message — do NOT list problems
- ONE question — do NOT ask multiple questions
- NEVER mention price
- NEVER say "I was browsing" — sounds fake
- ALWAYS use the business name, NEVER say "your business"
- NEVER mention Orion Flow by name in DMs — only in email

INSTAGRAM DM STRUCTURE (max 4 lines):
- Line 1: Specific observation about them (something only someone who looked would know)
- Line 2: One specific pain related to what you observed
- Line 3: One simple, direct question
- NO call to action — just open a conversation

FACEBOOK DM STRUCTURE (max 4 lines):
- Line 1: Local connection + specific observation
- Line 2: Question about how they handle that specific problem
- Line 3: Curiosity — not an offer
- Slightly more formal than Instagram

EMAIL STRUCTURE (max 6 lines):
- Subject: short, specific, no clickbait (e.g. "Quick question about [Business Name]")
- Line 1: Specific observation about them
- Line 2: One specific pain you identified
- Line 3-4: Local social proof (e.g. "helped another Danbury shop...")
- Line 5: Simple, zero-pressure call to action
- Line 6: Casual signature

EXAMPLES OF WHAT WORKS vs DOESN'T:

❌ WEAK — generic, any business could receive this:
"Hey! I noticed your business doesn't have a website. I help local businesses get more customers online. Interested in a free demo?"

✅ STRONG — specific, feels written just for them:
"Hey — your reviews mention people love the fades but I saw a few say they couldn't find how to book online. How are you handling that right now?"

❌ WEAK — tries to sell in the first message:
"We build websites that can double your bookings. Would love to show you our packages."

✅ STRONG — opens conversation with a question:
"Noticed [Business Name] doesn't show up when I search 'barbershop Danbury' on Google. Are you getting most of your customers from word of mouth?" """,

    'follow_up': """Current funnel stage: FOLLOW-UP (Middle of Funnel)

OBJECTIVE: Add value. Build trust. Create soft urgency. Still NOT selling directly.

GOLDEN RULES:
- Reference that you reached out before (one line, not apologetic)
- ONE new piece of value: a stat, a local example, a competitor observation
- Local social proof: "Other shops in Danbury have been..."
- Create FOMO without pressure: competitors are already doing this
- ONE question per message — low-commitment ask
- Use the business name, never "your business"
- NEVER list multiple benefits — pick the ONE most relevant

INSTAGRAM DM (max 4 lines):
- Line 1: Brief reference to previous message
- Line 2: ONE new value point or competitor observation
- Line 3: One simple question
- Casual, not desperate

FACEBOOK DM (max 4 lines):
- Same structure, slightly more detailed
- End with low-commitment ask

EMAIL (max 6 lines):
- Subject: value-forward, not "following up" (e.g. "Saw something about [Business Name]")
- Include ONE specific stat or proof point
- End with a soft ask (not "hop on a call")""",

    'closing': """Current funnel stage: CLOSING (Bottom of Funnel)

OBJECTIVE: Get a meeting. Make it dead simple to say yes.

GOLDEN RULES:
- Be direct: "10-min call this week"
- Mention specifically what you'd show/build for THEM (not generic)
- Use their business name
- Make the CTA concrete: suggest a specific day/time window
- ONE simple ask, ONE concrete next step
- Reference Orion Flow by name in email only

INSTAGRAM DM (max 4 lines):
- Line 1: Direct — you've been talking, time for next step
- Line 2: What you'd show them specifically
- Line 3: Concrete time suggestion
- Confident, not pushy

FACEBOOK DM (max 4 lines):
- Same but slightly more formal

EMAIL (max 6 lines):
- Subject: clear and direct (e.g. "Quick idea for [Business Name]")
- Mini-proposal feel: what you'd do for them specifically
- Concrete CTA with day/time""",

    'recovery': """Current funnel stage: RECOVERY (Re-engagement)

OBJECTIVE: Acknowledge the no. Understand the real objection. Leave the door open.

GOLDEN RULES:
- NEVER be aggressive, guilt-trip, or desperate
- Acknowledge and RESPECT their decision first
- Ask ONE question about the real objection (timing? value? budget? bad timing?)
- Present ONE specific pain they're still experiencing
- Show ONE concrete benefit — not a feature dump
- End with ZERO pressure, door genuinely open
- Sound like a neighbor checking in, not a salesperson
- Use their business name

INSTAGRAM DM (max 4 lines):
- Line 1: Acknowledge their decision respectfully
- Line 2: ONE genuine curiosity question
- Line 3: Door open, no pressure

FACEBOOK DM (max 4 lines):
- Same structure, genuine tone

EMAIL (max 6 lines):
- Subject: casual, not "circling back" (e.g. "No worries, [Name]")
- Genuine acknowledgment + one curiosity question
- One specific pain point reminder
- Zero-pressure close""",

    'review_request': """Current funnel stage: POST-SALE (Review & Referral)

OBJECTIVE: Ask for a Google review AND a referral to other local businesses.

GOLDEN RULES:
- Be genuinely grateful — mention something specific about working with them
- Make it feel personal, not automated
- Keep it short — don't over-explain
- Use their business name
- Make the review ask EASY — say you'll send the direct link

INSTAGRAM DM (max 4 lines):
- Quick, grateful, personal
- One ask per message (review OR referral, not both)

FACEBOOK DM (max 4 lines):
- Same approach

EMAIL (max 6 lines):
- Subject: warm and specific (e.g. "Loved working with [Business Name]")
- Combines review + referral ask naturally
- Makes both super easy to do""",

    'referral_request': """Current funnel stage: POST-SALE (Referral Request)

OBJECTIVE: Ask for referrals to other local businesses they know.

GOLDEN RULES:
- Be genuinely grateful — mention something specific about working with them
- Make it feel personal, not automated
- Keep it short — don't over-explain
- Use their business name
- Ask if they know other business owners in Danbury who might need help
- Make it easy — "just send me their name or Instagram"

INSTAGRAM DM (max 4 lines):
- Quick, grateful, personal
- One ask: do they know other local business owners?

FACEBOOK DM (max 4 lines):
- Same approach, slightly more formal

EMAIL (max 6 lines):
- Subject: warm and specific (e.g. "Quick favor, [Business Name]")
- Grateful opener + specific referral ask
- Make it super easy to refer"""
}


def generate_funnel_message(lead, stage, api_key, previous_messages=None):
    """Generate messages for a single lead at a specific funnel stage.
    
    Args:
        lead: dict with lead data
        stage: funnel stage key (first_contact, follow_up, closing, recovery, review_request, referral_request)
        api_key: Anthropic API key
        previous_messages: list of previous message dicts from DB (optional)
    
    Returns dict with channel keys or None on failure.
    """
    # Build previous messages context
    prev_msgs_text = 'None — this is the first message.'
    if previous_messages:
        lines = []
        for msg in previous_messages:
            stage_label = msg.get('funnel_stage', 'unknown')
            channel = msg.get('channel', '')
            content = msg.get('message', '')
            created = msg.get('created_at', '')
            lines.append(f"[{stage_label}] [{channel}] ({created}): {content}")
        if lines:
            prev_msgs_text = '\n'.join(lines)

    # Build notes context
    notes_text = (lead.get('notas', '') or '').strip()
    if not notes_text:
        notes_text = 'No manual observations recorded yet.'

    # Get the stage-specific instruction
    stage_instruction = STAGE_PROMPTS.get(stage, STAGE_PROMPTS['first_contact'])

    # Map status name for the prompt
    stage_display_map = {
        'first_contact': 'FIRST CONTACT',
        'follow_up': 'FOLLOW-UP',
        'closing': 'CLOSING',
        'recovery': 'RECOVERY',
        'review_request': 'POST-SALE (Review Request)',
        'referral_request': 'POST-SALE (Referral Request)',
    }

    prompt = f"""{stage_instruction}

Business data:
- Name: {lead.get('nome_negocio', '')}
- Type: {lead.get('categoria', '')}
- Location: {lead.get('endereco', '')}
- Website: {lead.get('site', '') or 'none'}
- Instagram: {lead.get('instagram', '') or 'none'}
- Facebook: {lead.get('facebook', '') or 'none'}
- Google rating: {lead.get('avaliacao_google', '')}
- Number of reviews: {lead.get('num_reviews', '')}
- Auto-detected pain point: {lead.get('justificativa_score', '')}
- Quality score: {lead.get('score_prioridade', '')}/10

Manual observations from research:
{notes_text}

Previous messages sent:
{prev_msgs_text}

IMPORTANT: Use the manual observations above as the PRIMARY source for identifying weaknesses and pain points. These are real notes from manually visiting their Instagram, Facebook and website. Always prioritize this information over automatically collected data.

If notes mention:
- Inactive Instagram → focus message on missing customers online
- Broken or slow website → focus on losing customers before they book
- No Facebook presence → focus on invisible to local searches
- No booking system → focus on missed revenue after hours
- Old posts → focus on appearing unprofessional to new customers

I work at Orion Flow — we build professional websites + online booking + AI chatbot for small local businesses in Danbury, CT.

Return ONLY a valid JSON object (no markdown, no explanation):
{{
  "dm_instagram": "the Instagram DM message",
  "dm_facebook": "the Facebook DM message",
  "email_subject": "short compelling subject line",
  "email_body": "the email body"
}}

LANGUAGE & TONE RULES:
- American English only
- Extremely human — like a real neighbor helping
- Natural contractions: I've, you're, don't, can't
- Never: 'I hope this finds you well', 'touching base', 'synergy', 'solutions', 'leverage', 'excited to connect'
- End casually: 'Let me know!', 'Talk soon!', 'Would love to chat!'
- Maximum 4 lines for DMs
- Maximum 6 lines for email
- Never sell on first contact
- One pain point per message — never list problems
- One question per message — never multiple questions
- Always use real business name, never 'your business'
- Always mention Danbury CT for local connection

MESSAGE QUALITY CHECK before returning:
- Would a real person send this exact message?
- Does it mention something specific about this business that shows research?
- Does it create curiosity without revealing everything?
- Is it under 4 lines for DM?
- Does it avoid ALL corporate language?
- Would YOU respond to this if you received it?
If any answer is NO — rewrite until all YES."""

    payload = json.dumps({
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 800,
        'system': SYSTEM_PROMPT_B2B,
        'messages': [{'role': 'user', 'content': prompt}]
    }).encode('utf-8')

    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            response = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise Exception('Claude API Key inválida')
        error_body = ''
        try:
            error_body = e.read().decode('utf-8', errors='replace')[:200]
        except Exception:
            pass
        raise Exception(f'Claude API erro HTTP {e.code}: {error_body}')

    content = response.get('content', [])
    if not content:
        return None

    text = content[0].get('text', '')

    # Parse JSON response
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        import re
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
            except json.JSONDecodeError:
                return None
        else:
            return None

    # Normalize into our format
    messages = {}
    if 'dm_instagram' in result:
        messages['Instagram DM'] = result['dm_instagram']
    if 'dm_facebook' in result:
        messages['Facebook DM'] = result['dm_facebook']
    if 'email_subject' in result or 'email_body' in result:
        messages['Email'] = {
            'subject': result.get('email_subject', ''),
            'body': result.get('email_body', '')
        }

    return messages if messages else None

