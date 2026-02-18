import requests
import datetime
import json
import time
import sqlite3

DB = "identity.db"
GRAPH_VERSION = "v19.0"
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"

def db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def get_state(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source='facebook_ads'
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        return {
            "last_sync_date": None
        }

    return json.loads(row[0])


def save_state(uid, state):

    con = db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, 'facebook_ads', ?, ?)
    """, (
        uid,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()

def get_ads_connection(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT ad_account_id, access_token
        FROM facebook_ads_connections
        WHERE uid=?
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Facebook Ads not connected")

    return row[0], row[1]

def safe_get(url, params):

    while True:
        r = requests.get(url, params=params, timeout=60)

        if r.status_code == 429:
            time.sleep(2)
            continue

        data = r.json()

        # token expired
        if isinstance(data, dict) and data.get("error", {}).get("code") == 190:
            raise Exception("Facebook token expired")

        return data

def sync_facebook_ads(uid, sync_type="historical"):

    ad_account_id, access_token = get_ads_connection(uid)
    state = get_state(uid)

    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    if sync_type == "incremental" and state.get("last_sync_date"):
        since_date = state.get("last_sync_date")
    else:
        since_date = (today - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    con = db()
    cur = con.cursor()
    now = datetime.datetime.utcnow().isoformat()

    total_campaigns = 0
    total_adsets = 0
    total_ads = 0
    total_creatives = 0
    total_insights = 0
    rows = []

    url = f"{GRAPH_BASE}/{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,objective,daily_budget,start_time,stop_time",
        "limit": 100
    }

    while url:
        data = safe_get(url, params)

        for campaign in data.get("data", []):
            total_campaigns += 1

            cur.execute("""
                INSERT OR REPLACE INTO facebook_ad_campaigns
                (uid, campaign_id, account_id, name, status, objective,
                 daily_budget, start_time, stop_time, raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                campaign.get("id"),
                ad_account_id,
                campaign.get("name"),
                campaign.get("status"),
                campaign.get("objective"),
                campaign.get("daily_budget"),
                campaign.get("start_time"),
                campaign.get("stop_time"),
                json.dumps(campaign),
                now
            ))

            rows.append({"type": "campaign", "id": campaign.get("id")})

        url = data.get("paging", {}).get("next")
        params = None

    url = f"{GRAPH_BASE}/{ad_account_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,campaign_id,daily_budget,start_time,end_time",
        "limit": 100
    }

    while url:
        data = safe_get(url, params)

        for adset in data.get("data", []):
            total_adsets += 1

            cur.execute("""
                INSERT OR REPLACE INTO facebook_ad_sets
                (uid, adset_id, campaign_id, name, status,
                 daily_budget, start_time, end_time, raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                adset.get("id"),
                adset.get("campaign_id"),
                adset.get("name"),
                adset.get("status"),
                adset.get("daily_budget"),
                adset.get("start_time"),
                adset.get("end_time"),
                json.dumps(adset),
                now
            ))

            rows.append({"type": "adset", "id": adset.get("id")})

        url = data.get("paging", {}).get("next")
        params = None

    url = f"{GRAPH_BASE}/{ad_account_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,adset_id,creative{id,name,object_story_id}",
        "limit": 100
    }

    while url:
        data = safe_get(url, params)

        for ad in data.get("data", []):
            total_ads += 1

            creative = ad.get("creative", {})

            cur.execute("""
                INSERT OR REPLACE INTO facebook_ads
                (uid, ad_id, adset_id, name, status,
                 creative_id, raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                uid,
                ad.get("id"),
                ad.get("adset_id"),
                ad.get("name"),
                ad.get("status"),
                creative.get("id"),
                json.dumps(ad),
                now
            ))

            if creative.get("id"):
                total_creatives += 1
                cur.execute("""
                    INSERT OR REPLACE INTO facebook_ad_creatives
                    (uid, creative_id, name, object_story_id,
                     raw_json, fetched_at)
                    VALUES (?,?,?,?,?,?)
                """, (
                    uid,
                    creative.get("id"),
                    creative.get("name"),
                    creative.get("object_story_id"),
                    json.dumps(creative),
                    now
                ))

            rows.append({"type": "ad", "id": ad.get("id")})

        url = data.get("paging", {}).get("next")
        params = None

    # ------------------------------------------------
    # 4️⃣ INSIGHTS (AD LEVEL)
    # ------------------------------------------------

    url = f"{GRAPH_BASE}/{ad_account_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "campaign_id,adset_id,ad_id,impressions,clicks,spend,ctr,cpc,cpm,reach,date_start,date_stop",
        "level": "ad",
        "time_range": json.dumps({
            "since": since_date,
            "until": today_str
        }),
        "limit": 100
    }

    while url:
        data = safe_get(url, params)

        for insight in data.get("data", []):
            total_insights += 1

            cur.execute("""
                INSERT INTO facebook_ads_insights
                (uid, account_id, campaign_id, adset_id, ad_id,
                 date_start, date_stop,
                 impressions, clicks, spend,
                 ctr, cpc, cpm, reach,
                 raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                ad_account_id,
                insight.get("campaign_id"),
                insight.get("adset_id"),
                insight.get("ad_id"),
                insight.get("date_start"),
                insight.get("date_stop"),
                insight.get("impressions"),
                insight.get("clicks"),
                insight.get("spend"),
                insight.get("ctr"),
                insight.get("cpc"),
                insight.get("cpm"),
                insight.get("reach"),
                json.dumps(insight),
                now
            ))

            rows.append({"type": "insight", "ad_id": insight.get("ad_id")})

        url = data.get("paging", {}).get("next")
        params = None

    con.commit()
    con.close()

    # Update state
    state["last_sync_date"] = today_str
    save_state(uid, state)

    return {
        "campaigns": total_campaigns,
        "adsets": total_adsets,
        "ads": total_ads,
        "creatives": total_creatives,
        "insights": total_insights,
        "rows": rows
    }