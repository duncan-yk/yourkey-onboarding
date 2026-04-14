#!/usr/bin/env python3
"""
YKM Guest Conversation KB Backfill Script
Processes 500 threads per run from the Hostfully CSV export.
Tracks progress in kb_backfill_cursor.txt.
Run daily via cron: 0 4 * * * /usr/bin/python3 /home/ykm/automation/kb_backfill.py
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
CSV_PATH = '/home/ykm/message_data.csv'
CURSOR_FILE = '/home/ykm/automation/kb_backfill_cursor.txt'
BATCH_SIZE = 500
MIN_MESSAGES = 20
CUTOFF_DATE = datetime(2022, 4, 14)

NOTION_TOKEN = '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'
NOTION_KB_DB = '46df3693-7f54-4571-b1f2-6900f4e1b7b9'

ANTHROPIC_KEY_FILE = '/home/ykm/automation/.anthropic_key'

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def load_anthropic_key():
    if os.path.exists(ANTHROPIC_KEY_FILE):
        return open(ANTHROPIC_KEY_FILE).read().strip()
    # Try environment
    return os.environ.get('ANTHROPIC_API_KEY', '')

def notion_request(path, body):
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(
        f'https://api.notion.com{path}',
        data=data,
        headers={
            'Authorization': f'Bearer {NOTION_TOKEN}',
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

def claude_analyze(conversation, anthropic_key):
    prompt = f"""Analyze this guest conversation for a short-term rental property management company.

{conversation}

Respond ONLY with valid JSON, no markdown:
{{
  "kb_worthy": false,
  "score": 1,
  "category": "Other",
  "title": "title here",
  "problem": "what was the problem",
  "outcome": "what happened",
  "learning": "what staff should learn",
  "summary": "brief summary",
  "tags": ["tag1"],
  "resolution": "how it was resolved",
  "sentiment": "Neutral",
  "escalated": false,
  "messageCount": 0,
  "conversationSource": "Other"
}}

Score 9-10: critical recurring issues. 7-8: common important lessons. 5-6: useful situational knowledge. 3-4: routine minor learning. 1-2: purely transactional.
kb_worthy true if score >= 5. When in doubt score higher.
sentiment: Positive/Neutral/Frustrated/Angry
conversationSource: Airbnb/VRBO/Direct/Other"""

    body = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": prompt}]
    }
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=data,
        headers={
            'x-api-key': anthropic_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json'
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            result = json.loads(r.read())
            text = result['content'][0]['text']
            # Strip any markdown fences
            text = text.replace('```json', '').replace('```', '').strip()
            return json.loads(text)
    except Exception as e:
        print(f'  Claude error: {e}')
        return None

def write_to_notion(analysis, thread_uid, channel):
    category_map = {
        'Guest issue': 'Guest issue',
        'Maintenance': 'Maintenance',
        'Access / Check-in': 'Access / Check-in',
        'Building rule': 'Building rule',
        'Owner preference': 'Owner preference',
        'Supplies / Inventory': 'Supplies / Inventory',
        'Cleaning / Turnover': 'Cleaning / Turnover',
        'Other': 'Other'
    }
    category = category_map.get(analysis.get('category', 'Other'), 'Other')

    title = analysis.get('title', 'Untitled')[:80]
    prefix = f"[Backfill] {title}"

    extra = (
        f"Thread UID: {thread_uid}\n"
        f"Source: {analysis.get('conversationSource', 'Other')}\n"
        f"Sentiment: {analysis.get('sentiment', 'Neutral')}\n"
        f"Escalated: {analysis.get('escalated', False)}\n"
        f"Message Count: {analysis.get('messageCount', 0)}\n"
        f"Score: {analysis.get('score', 0)}\n"
        f"Problem: {analysis.get('problem', '')}\n"
        f"Outcome: {analysis.get('outcome', '')}"
    )

    body = {
        'parent': {'database_id': NOTION_KB_DB},
        'properties': {
            'Note': {'title': [{'text': {'content': prefix[:100]}}]},
            'Symptom / Question': {'rich_text': [{'text': {'content': analysis.get('problem', '')[:2000]}}]},
            'Resolution / Answer': {'rich_text': [{'text': {'content': analysis.get('resolution', '')[:2000]}}]},
            'Category': {'select': {'name': category}},
            'Confidence': {'select': {'name': 'Unverified'}},
            'Source': {'select': {'name': 'GRA'}},
            'Extra notes': {'rich_text': [{'text': {'content': extra[:2000]}}]}
        }
    }

    result = notion_request('/v1/pages', body)
    return result.get('object') != 'error'

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    anthropic_key = load_anthropic_key()
    if not anthropic_key:
        print('ERROR: No Anthropic API key found. Add it to /home/ykm/automation/.anthropic_key')
        sys.exit(1)

    print(f'[{datetime.now()}] Starting KB backfill run...')

    # Load cursor (which batch index to start from)
    cursor = 0
    if os.path.exists(CURSOR_FILE):
        cursor = int(open(CURSOR_FILE).read().strip())
    print(f'Resuming from thread index: {cursor}')

    # Load and group all threads from CSV
    print('Loading CSV and grouping threads...')
    threads = {}
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                created = datetime.fromisoformat(row['created'].replace('Z', ''))
                if created >= CUTOFF_DATE:
                    tid = row['threadUid']
                    if tid not in threads:
                        threads[tid] = {'messages': [], 'channel': row.get('channel', 'Other')}
                    threads[tid]['messages'].append(row)
            except Exception:
                pass

    # Filter to 20+ messages only
    large_threads = [(tid, data) for tid, data in threads.items() if len(data['messages']) >= MIN_MESSAGES]
    large_threads.sort(key=lambda x: x[0])  # consistent ordering

    total = len(large_threads)
    batch = large_threads[cursor:cursor + BATCH_SIZE]

    if not batch:
        print(f'All {total} threads have been processed! Backfill complete.')
        open(CURSOR_FILE, 'w').write('0')  # reset
        return

    print(f'Processing batch {cursor}-{cursor+len(batch)} of {total} threads...')

    kb_written = 0
    skipped = 0
    errors = 0

    for i, (thread_uid, data) in enumerate(batch):
        msgs = sorted(data['messages'], key=lambda x: x.get('created', ''))
        channel = data['channel']

        # Build conversation text
        lines = []
        for m in msgs:
            sender = m.get('senderType', 'UNKNOWN')
            text = m.get('text', '') or ''
            if text and text != 'NULL':
                lines.append(f"{sender}: {text}")

        conversation = '\n'.join(lines)[:8000]  # cap at 8k chars

        print(f'  [{i+1}/{len(batch)}] Thread {thread_uid[:8]}... ({len(msgs)} msgs)', end='', flush=True)

        # Analyze with Claude
        analysis = claude_analyze(conversation, anthropic_key)
        if not analysis:
            print(' ERROR')
            errors += 1
            time.sleep(2)
            continue

        analysis['messageCount'] = len(msgs)

        if analysis.get('kb_worthy') and analysis.get('score', 0) >= 5:
            success = write_to_notion(analysis, thread_uid, channel)
            if success:
                kb_written += 1
                print(f' ✓ KB (score {analysis.get("score")})')
            else:
                print(f' NOTION ERROR')
                errors += 1
        else:
            skipped += 1
            print(f' skip (score {analysis.get("score", "?")})')

        # Rate limiting
        time.sleep(0.5)

    # Save cursor
    new_cursor = cursor + len(batch)
    open(CURSOR_FILE, 'w').write(str(new_cursor))

    print(f'\n✅ Run complete:')
    print(f'   Processed: {len(batch)} threads')
    print(f'   KB entries written: {kb_written}')
    print(f'   Skipped (not KB worthy): {skipped}')
    print(f'   Errors: {errors}')
    print(f'   Next run starts at index: {new_cursor} of {total}')
    remaining = total - new_cursor
    print(f'   Remaining: {remaining} threads (~{remaining // BATCH_SIZE} more runs)')

if __name__ == '__main__':
    main()
