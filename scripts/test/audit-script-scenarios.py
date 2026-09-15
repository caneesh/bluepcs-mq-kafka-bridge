#!/usr/bin/env python3
# Runs audit-gap-check.sh and abc-balance-check.sh against SQLite fixtures through
# scripts/test/fakehive.sh (a Hive-to-SQLite adapter). No Hive or Kafka needed:
#   python3 scripts/test/audit-script-scenarios.py
# Each scenario is a defect the scripts once missed; keep them green.
import os, sqlite3, subprocess, sys, json, datetime as dt
H=os.path.dirname(os.path.abspath(__file__)); ROOT=os.path.dirname(os.path.dirname(H))
now=dt.datetime.utcnow()
def iso(t): return t.strftime('%Y-%m-%dT%H:%M:%S')
def ago(**kw): return now-dt.timedelta(**kw)
# balance window exactly as the script computes it (lag 30 min, 1 hour wide)
WEND=(now-dt.timedelta(minutes=30)).replace(minute=0,second=0,microsecond=0)
WSTART=WEND-dt.timedelta(hours=1)
def in_window(seconds_after_start=60): return WSTART+dt.timedelta(seconds=seconds_after_start)

def build(name, rows):
    db=os.path.join(H,name+".db"); main=os.path.join(H,name+"-main.db")
    for f in (db,main):
        if os.path.exists(f): os.remove(f)
    c=sqlite3.connect(db)
    c.execute("CREATE TABLE bridge_audit_event(event_id TEXT, event_type TEXT, event_timestamp TEXT, event_dt TEXT, description TEXT, metadata_json TEXT)")
    c.execute("CREATE VIEW bridge_audit_event_deduped AS SELECT * FROM bridge_audit_event")
    c.execute("CREATE TABLE bridge_control_run(run_id,check_name,equation_no,stage_from,stage_to,window_start,window_end,expected_count,actual_count,variance,variance_pct,tolerance_pct,status,reason_code,detail,host,started_at,ended_at)")
    for (eid,et,t,meta,desc) in rows:
        c.execute("INSERT INTO bridge_audit_event VALUES (?,?,?,?,?,?)",(eid,et,iso(t),t.strftime('%Y-%m-%d'),desc,json.dumps(meta) if meta is not None else None))
    c.commit(); c.close()
    sqlite3.connect(main).close()
    return db, main

def run(script, db, main, env):
    e=dict(os.environ); e.update(env)
    e.update({"HIVE_CMD":os.path.join(H,"fakehive.sh"),"FAKEHIVE_DB":db,"FAKEHIVE_MAIN":main,"AUDIT_GAP_TABLE":"bluepcs.bridge_audit_event","ABC_CONTROL_TABLE":"bluepcs.bridge_control_run"})
    r=subprocess.run(["bash",os.path.join(ROOT,"scripts",script)],env=e,capture_output=True,text=True)
    return r.returncode, r.stdout+r.stderr

def ev(eid,et,t,meta=None,desc=""): return (eid,et,t,meta,desc)
results=[]
def check(label, code, expect, out, must=None):
    ok = code==expect and (must is None or all(m in out for m in must))
    results.append((ok,label,code,expect))
    print(("PASS " if ok else "FAIL ")+label+f" (exit {code}, expected {expect})")
    if not ok: print(out)

# ---------------- gap check ----------------
# G1: A completed+loaded; B completed, skipped, never loaded (3h ago) -> gap, exit 1
db,main=build("g1",[
  ev("A","MESSAGE_RECEIVED",ago(hours=3)), ev("A","PROCESSING_COMPLETED",ago(hours=3)), ev("A","HIVE_LOAD_COMPLETED",ago(hours=2,minutes=50)),
  ev("B","MESSAGE_RECEIVED",ago(hours=3)), ev("B","PROCESSING_COMPLETED",ago(hours=3)), ev("B","CLAIM_CHECK_SKIPPED",ago(hours=2,minutes=50)),
])
code,out=run("audit-gap-check.sh",db,main,{}); check("G1 skip-without-load is a gap",code,1,out,["SKIPPED-WITHOUT-LOAD: B"])
# G2: stuck: received 4h ago, failing every minute, last 1 min ago -> exit 2
rows=[ev("C","MESSAGE_RECEIVED",ago(hours=4)), ev("C","MESSAGE_PARSED",ago(hours=4))]
for m in range(0,240,10): rows.append(ev("C","ENRICHMENT_FAILED",ago(minutes=m+1)))
db,main=build("g2",rows)
code,out=run("audit-gap-check.sh",db,main,{}); check("G2 continuously failing message is stuck",code,2,out,["STUCK: C"])
# G3: empty table: silence check off -> 0 ; on -> 5
db,main=build("g3",[])
code,out=run("audit-gap-check.sh",db,main,{}); check("G3a empty table passes with silence check off",code,0,out)
code,out=run("audit-gap-check.sh",db,main,{"AUDIT_GAP_SILENCE_MINUTES":"60"}); check("G3b empty table is NO EVIDENCE with silence check on",code,5,out,["NO EVIDENCE"])
# G4: benign skip after load is not a gap
db,main=build("g4",[ev("A","MESSAGE_RECEIVED",ago(hours=3)), ev("A","PROCESSING_COMPLETED",ago(hours=3)), ev("A","HIVE_LOAD_COMPLETED",ago(hours=2,minutes=50)), ev("A","PROCESSING_COMPLETED",ago(hours=2)), ev("A","CLAIM_CHECK_SKIPPED",ago(hours=1,minutes=50))])
code,out=run("audit-gap-check.sh",db,main,{}); check("G4 skip after a load is benign",code,0,out)
# G5: poison discard with event_id is terminal (not stuck)
db,main=build("g5",[ev("P","MESSAGE_RECEIVED",ago(hours=4)), ev("P","MESSAGE_DISCARDED",ago(hours=3),{"errorCode":"POISON"})])
code,out=run("audit-gap-check.sh",db,main,{}); check("G5 poison discard is terminal",code,0,out)

# ---------------- balance check ----------------
full=lambda eid,t,pipe=None: [ev(eid,x,t+dt.timedelta(seconds=i),{"pipeline":pipe} if pipe else None) for i,x in enumerate(
    ["MESSAGE_RECEIVED","MESSAGE_PARSED","ENRICHMENT_COMPLETED","HDFS_WRITE_COMPLETED","KAFKA_PUBLISH_COMPLETED","PROCESSING_COMPLETED"])]
# B1: A,B complete; A loaded then skipped; B never loaded -> FAIL
rows=full("A",in_window(60))+full("B",in_window(120))+[ev("A","HIVE_LOAD_COMPLETED",in_window(300)), ev("A","PROCESSING_COMPLETED",in_window(600)), ev("A","CLAIM_CHECK_SKIPPED",in_window(660))]
db,main=build("b1",rows)
code,out=run("abc-balance-check.sh",db,main,{"ABC_TOLERANCE_PCT_HIVE_LOAD":"0"}); check("B1 B's missing load is not hidden by A's skip",code,1,out,["POSSIBLE_LOSS"])
# B2: hour boundary: received 30s before window end, completed 30s after -> PASS
t=WEND-dt.timedelta(seconds=30)
rows=[ev("D","MESSAGE_RECEIVED",t),ev("D","MESSAGE_PARSED",t+dt.timedelta(seconds=10)),ev("D","ENRICHMENT_COMPLETED",t+dt.timedelta(seconds=40)),ev("D","HDFS_WRITE_COMPLETED",t+dt.timedelta(seconds=50)),ev("D","KAFKA_PUBLISH_COMPLETED",t+dt.timedelta(seconds=55)),ev("D","PROCESSING_COMPLETED",t+dt.timedelta(seconds=60)),ev("D","HIVE_LOAD_COMPLETED",t+dt.timedelta(minutes=5))]
db,main=build("b2",rows)
code,out=run("abc-balance-check.sh",db,main,{}); check("B2 message crossing the hour boundary is not a loss",code,0,out)
# B3: healthy skip (loaded + skipped) and a second loaded message -> PASS
rows=full("A",in_window(60))+full("B",in_window(120))+[ev("A","HIVE_LOAD_COMPLETED",in_window(300)),ev("B","HIVE_LOAD_COMPLETED",in_window(310)),ev("A","CLAIM_CHECK_SKIPPED",in_window(660))]
db,main=build("b3",rows)
code,out=run("abc-balance-check.sh",db,main,{}); check("B3 benign duplicate skip passes",code,0,out)
# B4: empty window -> WARN NO_DATA (2); pass with ABC_EMPTY_WINDOW=pass
db,main=build("b4",[])
code,out=run("abc-balance-check.sh",db,main,{}); check("B4a empty window warns NO_DATA",code,2,out,["NO_DATA"])
code,out=run("abc-balance-check.sh",db,main,{"ABC_EMPTY_WINDOW":"pass"}); check("B4b empty window can be allowed",code,0,out)
# B5: pmm pipeline healthy incl. a redelivery resolved by the pre-check
t=in_window(60)
rows=[ev("E",x,t+dt.timedelta(seconds=i),{"pipeline":"pmm"}) for i,x in enumerate(["MESSAGE_RECEIVED","MESSAGE_PARSED","API_CALL_COMPLETED","HDFS_WRITE_COMPLETED","PROCESSING_COMPLETED"])]
rows+=[ev("F",x,t+dt.timedelta(seconds=100+i),{"pipeline":"pmm"}) for i,x in enumerate(["MESSAGE_RECEIVED","MESSAGE_PARSED","HDFS_WRITE_SKIPPED","PROCESSING_COMPLETED"])]
db,main=build("b5",rows)
code,out=run("abc-balance-check.sh",db,main,{"ABC_PIPELINE":"pmm"}); check("B5a healthy PMM traffic balances",code,0,out)
code,out=run("abc-balance-check.sh",db,main,{"ABC_PIPELINE":"bridge"}); check("B5b PMM traffic is invisible to the PMM+ run (NO_DATA, not loss)",code,2,out,["NO_DATA"])
# B6: resumed redelivery (no second ENRICHMENT_COMPLETED) balances
rows=full("A",in_window(60))[:4]+[ev("A","MESSAGE_RECEIVED",in_window(400)),ev("A","MESSAGE_PARSED",in_window(401)),ev("A","HDFS_WRITE_SKIPPED",in_window(402),{"reason":"resumed-from-landing"}),ev("A","KAFKA_PUBLISH_COMPLETED",in_window(403)),ev("A","PROCESSING_COMPLETED",in_window(404)),ev("A","HIVE_LOAD_COMPLETED",in_window(900))]
db,main=build("b6",rows)
code,out=run("abc-balance-check.sh",db,main,{}); check("B6 resumed redelivery balances",code,0,out)
# B7: poison discard inside the funnel is a drain, not a loss
rows=[ev("P","MESSAGE_RECEIVED",in_window(60)),ev("P","MESSAGE_DISCARDED",in_window(120),{"errorCode":"POISON","hdfsPath":"/e/P.json"})]
db,main=build("b7",rows)
code,out=run("abc-balance-check.sh",db,main,{}); check("B7 poison discard balances",code,0,out)

fails=[r for r in results if not r[0]]
print(f"\n{len(results)-len(fails)}/{len(results)} scenarios passed")
sys.exit(1 if fails else 0)
