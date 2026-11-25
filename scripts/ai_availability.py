#!/usr/bin/env python3
"""Shared, read-only AI availability projection. No model turns, retries or messages.
Sources: existing worker evidence and Codex account/rateLimits/read.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import re
import select
import subprocess
import time
from pathlib import Path
from zoneinfo import ZoneInfo

JST=ZoneInfo('Asia/Tokyo')
SCHEMA='kingdom.ai_availability.v1'
CODEX='/Applications/ChatGPT.app/Contents/Resources/codex'

def iso(t): return t.astimezone(JST).isoformat(timespec='seconds')
def parse_time(s):
    t=dt.datetime.fromisoformat(s)
    return t.replace(tzinfo=JST) if t.tzinfo is None else t

def classify(text, at):
    low=text.lower()
    if any(x in low for x in ("you've hit your limit",'usage limit','usage_limit_reached','rate limit','rate_limit_exceeded','too many requests')):
        reset=None
        m=re.search(r'resets\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*\(([^)]+)\)',text,re.IGNORECASE)
        if m:
            try:
                h=int(m[1]); minute=int(m[2] or 0)
                if not 1<=h<=12 or not 0<=minute<60: raise ValueError()
                local=at.astimezone(ZoneInfo(m[4]))
                reset=local.replace(hour=h%12+(12 if m[3].lower()=='pm' else 0),minute=minute,second=0,microsecond=0)
                if reset<=local: reset+=dt.timedelta(days=1)
            except (ValueError,KeyError): reset=None
        return {'state':'rate_limited','reason':'利用上限に到達','reset_at':iso(reset) if reset else None}
    if any(x in low for x in ('not logged in','organization has disabled','unauthorized','authentication failed','please run /login','token expired')):
        return {'state':'auth_required','reason':'認証またはアカウント状態の確認が必要','reset_at':None}
    return {'state':'execution_failed','reason':'実行失敗（利用上限とは未確定）','reset_at':None}

def tail(path):
    try:
        with path.open('rb') as f:
            f.seek(max(0,path.stat().st_size-262144))
            return f.read().decode('utf-8','replace').splitlines()
    except OSError: return []

def worker(participant, logs, now):
    name=participant.lower()
    path=logs/(name+'_auto_wake.log')
    events=[]
    for line in tail(path):
        m=re.match(r'^\[(\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] (.*)$',line)
        if not m: continue
        try:
            at=dt.datetime.strptime(str(now.year)+'-'+m[1],'%Y-%m-%d %H:%M:%S').replace(tzinfo=JST)
            if at>now+dt.timedelta(days=1): at=at.replace(year=at.year-1)
        except ValueError: continue
        body=m[2]
        if body.startswith(participant+'が答えた('):
            events.append((at,{'state':'response_observed','reason':'実応答を観測（作業の完了判定ではない）','reset_at':None},line))
        elif body.startswith('起きられなかった('): events.append((at,classify(body,at),line))
    autopsy=logs/(name+'_auto_wake_検死.json')
    try:
        raw=autopsy.read_bytes(); d=json.loads(raw); at=parse_time(d['at'])
        if d.get('rc')!=0:
            events.append((at,classify(str(d.get('stdout全文',''))+'\n'+str(d.get('stderr全文','')),at),raw.decode('utf-8')))
    except (OSError,ValueError,KeyError,TypeError): pass
    if not events:
        return {'state':'unknown','reason':'実行結果の証拠がない','observed_at':None,'reset_at':None,'scope':'Mac automatic worker'}
    # Autopsy is appended last and wins on equal time; it contains the real stdout.
    at,result,evidence=max(enumerate(events),key=lambda x:(x[1][0],x[0]))[1]
    result.update(observed_at=iso(at),scope='Mac automatic worker',evidence_path=str(autopsy if evidence.startswith('{') else path),evidence_sha256=hashlib.sha256(evidence.encode()).hexdigest())
    if result['state']=='rate_limited' and result['reset_at'] and now>=parse_time(result['reset_at']):
        result.update(state='recovery_unconfirmed',reason='解除予定を経過・実応答による復旧は未確認')
    elif result['state']=='response_observed' and (now-at).total_seconds()>900:
        result.update(state='unknown',reason='直近15分の実応答がない（停止を意味しない）')
    return result

def quota_read(binary=CODEX):
    p=subprocess.Popen([binary,'app-server'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    buf=b''
    def send(d): p.stdin.write((json.dumps(d)+'\n').encode()); p.stdin.flush()
    def recv(i):
        nonlocal buf
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            while b'\n' in buf:
                line,buf=buf.split(b'\n',1); d=json.loads(line)
                if d.get('id')==i:
                    if 'error' in d: raise RuntimeError('quota RPC error')
                    return d['result']
            if select.select([p.stdout],[],[],1)[0]:
                data=os.read(p.stdout.fileno(),65536)
                if not data: raise RuntimeError('quota connection closed')
                buf+=data
                if len(buf)>2_000_000: raise RuntimeError('quota response too large')
        raise TimeoutError('quota timeout')
    try:
        send({'id':0,'method':'initialize','params':{'clientInfo':{'name':'kingdom_availability','version':'1.0'}}}); recv(0)
        send({'method':'initialized'}); send({'id':1,'method':'account/rateLimits/read'})
        return recv(1)
    finally:
        p.terminate()
        try:p.wait(3)
        except subprocess.TimeoutExpired:p.kill();p.wait()
        p.stdin.close();p.stdout.close()

def quota_state(raw, now):
    buckets=raw.get('rateLimitsByLimitId')
    if not isinstance(buckets,dict) or not buckets:
        one=raw.get('rateLimits'); buckets={str(one.get('limitId') or 'legacy'):one} if isinstance(one,dict) else {}
    items=[]
    for key,b in buckets.items():
        if not isinstance(b,dict):continue
        windows=[]
        for w in ('primary','secondary'):
            v=b.get(w)
            if not isinstance(v,dict):continue
            used=v.get('usedPercent')
            if not isinstance(used,(int,float)) or isinstance(used,bool) or not math.isfinite(used):continue
            reset=v.get('resetsAt')
            windows.append({'window':w,'used_percent':used,'window_minutes':v.get('windowDurationMins'),'reset_at':iso(dt.datetime.fromtimestamp(reset,JST)) if isinstance(reset,(int,float)) and 0<reset<32503680000 else None})
        limited=bool(b.get('rateLimitReachedType')) or b.get('spendControlReached') is True or any(w['used_percent']>=100 for w in windows)
        items.append({'limit_id':str(key)[:80],'state':'rate_limited' if limited else ('quota_available' if windows else 'unknown'),'windows':windows})
    states=[i['state'] for i in items]
    state='unknown' if not states or 'unknown' in states else 'quota_available'
    if 'rate_limited' in states:state='rate_limited' if all(s=='rate_limited' for s in states) else 'partially_rate_limited'
    # App-server verifies ordinaryUsageAllowed against the active account.
    # A usage percentage or a reset estimate is not proof of permission.
    ordinary_allowed=raw.get('ordinaryUsageAllowed')
    if ordinary_allowed is False:
        state='rate_limited'
    elif ordinary_allowed is not True and state=='quota_available':
        state='unknown'
    return {'state':state,'reason':{'unknown':'利用量を判定できない','quota_available':'取得した利用枠は上限未到達（実行成功の保証ではない）','rate_limited':'取得した利用枠が上限到達','partially_rate_limited':'一部の利用枠が上限到達・モデル対応は未判定'}[state], 'observed_at':iso(now),'scope':'Codex account used by local CLI; per-bucket, not per-thread','buckets':items,'source':'account/rateLimits/read'}

def build(logs, now, quota, *, quota_observed_at=None):
    code=worker('Code',logs,now); codex=worker('Codex',logs,now)
    quota_at=quota_observed_at if quota_observed_at is not None else now
    q=quota_state(quota,quota_at) if quota is not None else {'state':'unknown','reason':'利用量取得失敗・上限未到達とは判断しない','scope':'local Codex account','observed_at':iso(now),'buckets':[]}
    codex_state=q['state']
    # Authentication and actual execution errors are not cured by quota proof.
    if codex['state'] in ('auth_required','execution_failed'):
        codex_state=codex['state']
    elif codex['state'] in ('rate_limited','recovery_unconfirmed'):
        # A fresh, account-verified positive read may supersede only an older
        # worker quota failure. An equal/newer failure keeps the veto.
        try:
            positive=(q['state']=='quota_available' and
                      quota.get('ordinaryUsageAllowed') is True and
                      parse_time(q['observed_at'])>parse_time(codex['observed_at']))
        except (ValueError, KeyError, TypeError, AttributeError):
            positive=False
        if not positive:
            codex_state=codex['state']
    return {'schema':SCHEMA,'observed_at':iso(now),'valid_until':iso(now+dt.timedelta(seconds=180)), 'monitor':'Mac ai_availability (no model execution)', 'participants':{'Code':dict(code,worker=code.copy()),'Codex':{'state':codex_state,'reason':codex['reason'] if codex_state==codex['state'] else q['reason'],'scope':'see worker and account_quota scopes','reset_at':codex.get('reset_at'),'worker':codex,'account_quota':q}},'policy':'状態札は実行権限ではない。停止中の相手への即時回答を前提にせず、既存案件を保全する。解除予定は復旧証拠ではない。'}

def code_queue_health(root, now):
    """newpcのCode受信箱の滞留だけを読む。本文やファイル名は共有しない。"""
    inbox=Path(root).parent/'code_inbox'
    try:
        pending=[p for p in inbox.iterdir() if p.is_file() and not p.is_symlink() and p.suffix=='.json']
    except OSError:
        return None
    if not pending:
        return {'pending_count':0,'oldest_pending_at':None,'oldest_age_seconds':0,'latest_done_at':None}
    stats=[]
    for p in pending:
        try: stats.append(p.stat().st_mtime)
        except OSError: pass
    if not stats:
        return None
    oldest=min(stats)
    latest_done=None
    done=inbox/'done'
    try:
        ds=[p.stat().st_mtime for p in done.iterdir() if p.is_file() and not p.is_symlink()]
        if ds: latest_done=max(ds)
    except OSError:
        pass
    return {
        'pending_count':len(stats),
        'oldest_pending_at':iso(dt.datetime.fromtimestamp(oldest,JST)),
        'oldest_age_seconds':max(0,int(now.timestamp()-oldest)),
        'latest_done_at':iso(dt.datetime.fromtimestamp(latest_done,JST)) if latest_done else None,
    }

def read_shared(root, now=None):
    now=now or dt.datetime.now(JST)
    try:
        d=json.loads((Path(root)/'status/ai_availability.json').read_text())
        if d.get('schema')!=SCHEMA or not isinstance(d.get('participants'),dict):raise ValueError('schema')
        age=(now-parse_time(d['observed_at'])).total_seconds()
        if not -30<=age<=180 or now>parse_time(d['valid_until']):
            return {'schema':SCHEMA,'state':'stale','reason':'稼働札の監視更新が古い・現在は未確認','last_observed_at':d.get('observed_at'),'participants':{k:{'state':'unknown','last_observation':v} for k,v in d['participants'].items()}}
        out=dict(d,state='fresh')
        q=code_queue_health(root,now)
        code=(out.get('participants') or {}).get('Code')
        if q is not None and isinstance(code,dict):
            code=dict(code); code['queue']=q
            if q.get('pending_count',0)>0 and q.get('oldest_age_seconds',0)>=1800 and code.get('state') in ('unknown','response_observed'):
                code.update(state='queue_stalled',reason='Code受信箱が30分以上処理されていない')
            participants=dict(out['participants']);participants['Code']=code;out['participants']=participants
        return out
    except (OSError,ValueError,KeyError,TypeError):return {'schema':SCHEMA,'state':'unknown','reason':'稼働札が未取得または破損','participants':{}}

def summary(d):
    if d.get('state')!='fresh':return '◆AI稼働札: '+d.get('reason','未確認')
    labels={'rate_limited':'利用上限で停止','partially_rate_limited':'一部利用枠が上限','quota_available':'利用枠は上限未到達','auth_required':'認証確認が必要','execution_failed':'実行失敗','recovery_unconfirmed':'解除予定経過・復旧未確認','response_observed':'実応答あり','queue_stalled':'受信箱滞留','unknown':'現在の実行可否は未確認'}
    rows=[]
    for name,v in d['participants'].items():
        s=name+'='+labels.get(v.get('state'),'未確認')
        reset=v.get('reset_at')
        if reset:s+='（解除予定 '+reset+'・実復旧は別確認）'
        rows.append(s)
    return '◆AI稼働札: '+' / '.join(rows)+'。CodeはMac自動起動、Codexは同CLI利用口座と自動起動の観測。'

def overlay(digest, root):
    """Replace a cached status line at read time; never retain an expired green line."""
    if not (Path(root)/'status/ai_availability.json').exists() and '◆AI稼働札:' not in digest:
        return digest
    lines=[x for x in digest.splitlines() if not x.startswith('◆AI稼働札:')]
    lines.insert(min(1,len(lines)),summary(read_shared(root)))
    return '\n'.join(lines)

def atomic(path,d):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name('.'+path.name+'.'+str(os.getpid())+'.tmp')
    with tmp.open('w') as f:json.dump(d,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--publish',action='store_true');a=ap.parse_args()
    logs=Path.home()/'Library/Logs'
    with (logs/'ai_availability.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return
        try:
            q=quota_read()
            read_at=dt.datetime.now(JST)
        except Exception:
            q=None
            read_at=None
        d=build(logs,dt.datetime.now(JST),q,quota_observed_at=read_at)
        atomic(logs/'ai_availability.json',d)
        if a.publish:
            receiver="import json,sys,os; from pathlib import Path; p=Path.home()/'local-ai-stack/status/ai_availability.json'; d=json.load(sys.stdin); assert d['schema']=='kingdom.ai_availability.v1'; t=p.with_name('.ai_availability.'+str(os.getpid())+'.tmp'); t.write_text(json.dumps(d,ensure_ascii=False,indent=2)); os.replace(t,p)"
            import shlex
            r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','newpc','python3 -c '+shlex.quote(receiver)],check=False, input=json.dumps(d,ensure_ascii=False),text=True,capture_output=True,timeout=15)
            if r.returncode:raise RuntimeError('shared state publish failed; previous record will expire')
        print(summary(dict(d,state='fresh')))
if __name__=='__main__':main()
