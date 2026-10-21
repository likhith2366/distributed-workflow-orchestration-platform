"""Generate the revised project brief from observed benchmark reports."""
from pathlib import Path
import json
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.units import inch

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "pdf" / "Conductor-Project-Brief.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
load = json.loads((ROOT / "benchmarks/local-10000.json").read_text())
chaos = json.loads((ROOT / "benchmarks/chaos.json").read_text())
GREEN = colors.HexColor("#193E30")
MUTED = colors.HexColor("#6C7F73")
PALE = colors.HexColor("#F0F4EC")
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="Kicker", fontName="Helvetica-Bold", fontSize=9, leading=13, textColor=MUTED, spaceAfter=18))
styles.add(ParagraphStyle(name="Hero", fontName="Helvetica", fontSize=34, leading=39, textColor=GREEN, spaceAfter=18))
styles.add(ParagraphStyle(name="Section", fontName="Helvetica-Bold", fontSize=18, leading=23, textColor=GREEN, spaceAfter=14))
styles.add(ParagraphStyle(name="Sub", fontName="Helvetica-Bold", fontSize=11, leading=15, textColor=GREEN, spaceBefore=13, spaceAfter=7))
styles.add(ParagraphStyle(name="Copy", fontName="Helvetica", fontSize=10.5, leading=16, textColor=GREEN, spaceAfter=10))
styles.add(ParagraphStyle(name="SmallCopy", fontName="Helvetica", fontSize=8.5, leading=12, textColor=MUTED, spaceAfter=8))
styles.add(ParagraphStyle(name="Cell", fontName="Helvetica", fontSize=9, leading=13, textColor=GREEN))
story = []
def p(text, style="Copy"):
    story.append(Paragraph(text, styles[style]))
def title(number, text):
    p("CONDUCTOR / ENGINEERING BRIEF / " + number, "Kicker")
    p(text, "Section")
def sub(text):
    p(text, "Sub")
def table(rows, widths):
    data = [[Paragraph(str(v), styles["Cell"]) for v in row] for row in rows]
    t = Table(data, colWidths=widths, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E2EBDD")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, PALE]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LINEBELOW", (0, 0), (-1, 0), .6, colors.HexColor("#C4D1BE")),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))
def page():
    story.append(PageBreak())
def footer(canvas, doc):
    canvas.setStrokeColor(colors.HexColor("#DCE4D7"))
    canvas.line(48, 45, 564, 45)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(48, 31, "CONDUCTOR  |  October 20, 2026  |  Local evidence, explicit limits")
    canvas.drawRightString(564, 31, str(doc.page))

p("PROJECT PORTFOLIO / JAVA 21 + TYPESCRIPT CDK", "Kicker")
p("Distributed Workflow<br/>Orchestration Platform", "Hero")
p("Durable execution for retail fulfillment", "Section")
p("A working orchestration engine that coordinates inventory, payment, fraud review, parallel packing, and dispatch through retries and worker failures.")
story.append(Spacer(1, 15))
table([
    ["MEASURED LOCAL THROUGHPUT", "PROCESS-CRASH RECOVERY"],
    [f"<b>{load['workflowsPerSecond']:.0f} workflows / second</b><br/>10,000 three-step workflows<br/>Synchronous durable writes",
     f"<b>{chaos['completed']} / {chaos['processKills']} completed</b><br/>Real child-JVM kills<br/>Zero duplicate ledger effects observed"],
], [258,258])
sub("Why this is relevant to Amazon")
p("The project exercises the service-engineering problems behind durable workflows: ambiguous outcomes, at-least-once attempts, downstream idempotency, lease fencing, version pinning, and operational recovery. The retail scenario makes those decisions concrete. This is an independent portfolio implementation, not an Amazon product.")
sub("What was built")
p("Java 21 coordinator; independent HTTP workers; checksummed event journal; DAG execution; durable retries, timers and approvals; compensation; idempotent effect ledger; browser console; and TypeScript CDK for an ECS/EFS demonstration.")
sub("What the current results establish")
p("The system works locally and has passed core, HTTP, worker-replacement, and crash-recovery tests. Infrastructure compiles and synthesizes. AWS deployment, DynamoDB/SQS integration, and production-scale claims remain future work.")
p("This revised brief replaces aspirational results with implemented behavior and reproducible measurements. The original source PDF is preserved in the repository.", "SmallCopy")

page()
title("02", "The execution model")
table([
    ["BOUNDARY", "IMPLEMENTED BEHAVIOR"],
    ["Durable transitions", "Clone state, append a framed event, flush it, then publish the new projection."],
    ["Worker ownership", "A claim persists owner, expiry, attempt, and increasing fencing token. Completion requires a matching live lease."],
    ["Dependencies", "DAG validation rejects cycles. Independent branches run concurrently; joins wait for all dependencies."],
    ["Retries and waits", "Backoff and due timestamps are persisted. Approval waits occupy no worker thread."],
    ["Recovery", "Replay validates sequence and checksum, then applies recorded projections. It does not repeat old activity calls."],
    ["Versioning", "Each run stores its complete definition. Later submissions cannot change an existing run."],
], [135,381])
sub("A crash that matters")
p("A worker writes a ledger effect and dies before recording task completion. The replacement waits for lease expiry, claims a higher token, and calls the ledger with the same execution/task key. The ledger returns the original effect; the coordinator accepts only the replacement's completion.")
sub("The exact guarantee")
p("Fencing protects orchestration state. The ledger protects its own effects by atomically persisting the key and value. Arbitrary payment or inventory services need their own idempotency transaction or provider contract; an engine cannot impose exactly-once behavior on an unrelated API.")
sub("Compensation after partial failure")
p("Stop pending tasks, drain already-running branches, then undo declared successful steps in reverse completion order. Compensation failures are explicit. Cancellation stops scheduling and fences completions; it does not roll back external work.")

page()
title("03", "Measurements you can defend")
p("Windows 11; Java 21.0.2; eight JVM-visible processors; 512 MiB maximum heap; eight embedded workers; 32 workflows maximum in flight. Each workflow performs two durable ledger effects and one echo. The workstation was not isolated from other development activity.")
table([
    ["10,000-WORKFLOW RUN", "OBSERVED"],
    ["Elapsed time, including startup and shutdown", f"{load['seconds']:.2f} seconds"],
    ["Completed workflows / ledger effects", "10,000 / 20,000"],
    ["Throughput", f"{load['workflowsPerSecond']:.2f} workflows/second"],
    ["Workflow p50 / p99", f"{load['workflowP50Ms']} ms / {load['workflowP99Ms']} ms"],
    ["Ready-to-claim p99", f"{load['workersMetrics']['dispatchP99Ms']} ms"],
    ["Durable transitions / event-log bytes", f"{load['engineMetrics']['durableTransitions']:,} / {load['journalBytes']:,}"],
    ["Worker errors / stale-write rejections", "0 / 2"],
], [335,181])
sub("Read the two rejections correctly")
p("The load run recorded 30,002 claims and 30,000 completed steps. All workflows completed. The two rejections are consistent with reclaimed attempts, but aggregate metrics do not identify the cause. Lock contention, scheduling, and host load remain hypotheses.")
sub("Methodology and limits")
p("This is a closed-loop engine benchmark, excluding HTTP and AWS. Every acknowledged transition and new effect flushes to disk. No warm-up is discarded. Percentiles use nearest rank. The measurements do not establish an open-loop SLO, maximum capacity, or an AWS performance result.")
p("Raw evidence: benchmarks/local-1000.json and benchmarks/local-10000.json.<br/>Reproduce: java -Xmx512m -cp build/classes dev.conductor.Benchmark 10000 32 8 benchmarks/local-10000.json", "SmallCopy")

page()
title("04", "Failure evidence and engineering judgment")
table([
    ["SEEDED CRASH BOUNDARY", "KILLS"],
    ["After execution creation", "55"],
    ["After task claim", "52"],
    ["After ledger effect, before completion", "49"],
    ["After completion acknowledgment", "44"],
    ["Total child JVMs forcibly terminated", "200"],
], [390,126])
p(f"<b>{chaos['completed']}/200 workflows completed; zero duplicate ledger effects observed.</b> Recovery p50 was {chaos['recoveryP50Ms']} ms and p99 was {chaos['recoveryP99Ms']} ms, using a 150 ms test lease. The running service uses a three-second lease.")
sub("What broke first")
p("The first chaos run could not reopen a journal immediately after the child exited: Windows briefly retained the file lock. The harness now retries that specific condition, with a five-second deadline. Recovery timing includes the delay. Corrupt checksums still fail immediately.")
sub("Additional checks")
p("Eighteen core tests cover replay, DAG joins, lease fencing, heartbeat renewal, retries, timers, compensation, duplicate effects, concurrent claims, cancellation, and torn/corrupt journals. HTTP integration verifies authentication, approvals, remote heartbeats, and replacement of a killed independent worker.")
sub("What was not injected")
p("No power loss, storage loss, network partition, clock-skew campaign, or AWS availability-zone failure. The 200-kill harness terminates a coordinator and embedded worker together. A separate integration test exercises an independent worker failure.")
p("Raw evidence: benchmarks/chaos.json. Seed: 20261020.<br/>Reproduce: java -Xmx512m -cp build/classes dev.conductor.ChaosHarness 200 20261020", "SmallCopy")

page()
title("05", "AWS path and interview narrative")
sub("Included deployment definition")
p("TypeScript CDK defines private ECS Fargate services, one coordinator, two worker tasks, encrypted EFS with an access point, a generated Secrets Manager token, an internal ALB, CloudWatch logs, a CPU alarm, and a dashboard. Compilation, infrastructure assertions, and synthesis passed. It has not been deployed.")
sub("Important current limits")
p("The coordinator is a single writer. Every event contains a full projection, all retained runs load into memory, and active timers are scanned. There is no journal compaction, multi-tenant authorization, or managed-store adapter. The demo's internal HTTP transport needs TLS before sensitive use. Docker execution remains unverified.")
sub("Managed-service migration")
p("Use a DynamoDB transaction to append the event, advance the execution version, validate fencing, and write an outbox entry together. Dispatch the outbox to SQS at least once. Add sharded due-timer queries, EventBridge long waits, and immutable S3 payload references. Validate these integrations before claiming them as implemented.")
sub("Evidence-backed resume wording")
p(f"Built Java 21 durable DAG orchestration with fenced leases, persisted retries, approvals, and compensation; completed 10,000 three-step workflows at {load['workflowsPerSecond']:.0f} workflows/s and {load['workersMetrics']['dispatchP99Ms']} ms p99 ready-to-claim latency in a local eight-worker benchmark.")
p(f"Validated recovery across 200 seeded child-JVM kills, completing 200/200 workflows with zero duplicate ledger effects observed and {chaos['recoveryP50Ms']} ms median recovery under a 150 ms test lease.")
sub("Be ready to explain")
p("Why a stable effect key excludes the attempt number. Why fencing cannot undo an external request. Why a dual DynamoDB/SQS write needs an outbox. Why draining branches matters before compensation. Why local throughput is not an AWS capacity claim.")
p("Primary AWS references: SQS standard-queue at-least-once delivery; DynamoDB transaction APIs and client-token lifetime; ECS EFS volume configuration. Direct links and deployment notes are in docs/aws.md. Full limitations and interview material are in the repository.", "SmallCopy")

doc = SimpleDocTemplate(str(OUT), pagesize=(612,792), rightMargin=48,leftMargin=48,
                        topMargin=48,bottomMargin=62,title="Conductor | Distributed Workflow Orchestration",
                        author="Conductor Project",pageCompression=1)
doc.build(story,onFirstPage=footer,onLaterPages=footer)
print(OUT)
