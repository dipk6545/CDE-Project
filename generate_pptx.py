import pptx
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)
blank_slide_layout = prs.slide_layouts[6]

DARK_BLUE = RGBColor(26, 54, 93)     # #1A365D Primary Header
ACCENT_BLUE = RGBColor(43, 108, 176) # #2B6CB0
CHARCOAL = RGBColor(45, 55, 72)      # #2D3748 Body text
LIGHT_BG = RGBColor(247, 250, 252)   # #F7FAFC Card Background
BORDER_COL = RGBColor(226, 232, 240) # #E2E8F0
WHITE = RGBColor(255, 255, 255)

def add_header(slide, title_text, category_text):
    tb_cat = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(11.5), Inches(0.4))
    tf_cat = tb_cat.text_frame
    tf_cat.word_wrap = True
    p_cat = tf_cat.paragraphs[0]
    p_cat.text = category_text.upper()
    p_cat.font.size = Pt(11)
    p_cat.font.bold = True
    p_cat.font.color.rgb = ACCENT_BLUE

    tb_title = slide.shapes.add_textbox(Inches(0.8), Inches(0.7), Inches(11.5), Inches(0.8))
    tf_title = tb_title.text_frame
    tf_title.word_wrap = True
    p_title = tf_title.paragraphs[0]
    p_title.text = title_text
    p_title.font.size = Pt(24)
    p_title.font.bold = True
    p_title.font.color.rgb = DARK_BLUE

# ==========================================
# SLIDE 1: Title Slide
# ==========================================
s1 = prs.slides.add_slide(blank_slide_layout)
card = s1.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(1.5), Inches(1.2), Inches(10.333), Inches(5.1))
card.fill.solid()
card.fill.fore_color.rgb = DARK_BLUE
card.line.color.rgb = DARK_BLUE

tb = s1.shapes.add_textbox(Inches(2.0), Inches(2.0), Inches(9.333), Inches(3.5))
tf = tb.text_frame
tf.word_wrap = True

p1 = tf.paragraphs[0]
p1.text = "Transit Lakehouse Pipeline"
p1.font.size = Pt(40)
p1.font.bold = True
p1.font.color.rgb = WHITE
p1.alignment = PP_ALIGN.CENTER

p2 = tf.add_paragraph()
p2.text = "Real-Time & Batch Cloud Data Engineering Platform"
p2.font.size = Pt(20)
p2.font.color.rgb = RGBColor(226, 232, 240)
p2.alignment = PP_ALIGN.CENTER
p2.space_before = Pt(14)

p3 = tf.add_paragraph()
p3.text = "Technologies: Apache Kafka • AWS S3 • Apache PySpark • Apache Airflow on K8s • PostgreSQL • Grafana Cloud"
p3.font.size = Pt(13)
p3.font.color.rgb = RGBColor(203, 213, 225)
p3.alignment = PP_ALIGN.CENTER
p3.space_before = Pt(36)

p4 = tf.add_paragraph()
p4.text = "Author: Cloud Data Engineering Team | November 2026"
p4.font.size = Pt(12)
p4.font.color.rgb = RGBColor(148, 163, 184)
p4.alignment = PP_ALIGN.CENTER
p4.space_before = Pt(14)

# ==========================================
# SLIDE 2: Problem Statement & Architecture Flowchart
# ==========================================
s2 = prs.slides.add_slide(blank_slide_layout)
add_header(s2, "End-to-End Pipeline Architecture & Flow", "System Flowchart")

flow_steps = [
    ("1. Ingestion Layer", "MBTA REST API v3\n• Public Transit Vehicles\n• Kafka Producer\n  (streaming/producer.py)\n• Kafka Broker (KRaft)\n  Topic: transit-vehicles"),
    ("2. Streaming Sink", "Kafka Consumer\n  (streaming/consumer.py)\n• Dual-mode buffering\n• 50 records / 60s\n• AWS S3 Bronze Storage\n  (Sydney ap-southeast-2)"),
    ("3. Lakehouse Medallion", "Apache PySpark 3.5.3\n• Bronze: Raw JSONL\n• Silver: Cleaned Parquet\n  (to_silver.py)\n• Gold: KPI Aggregates\n  (to_gold.py)"),
    ("4. Orchestration", "Apache Airflow 2.10.3\n• Kubernetes (Kind)\n• KubernetesExecutor\n• 4-stage pipeline DAG\n• Serving DB:\n  Prisma PostgreSQL"),
    ("5. Observability", "Grafana Cloud & Local\n• Executive Transit Control\n• Live Speed & Status\n• Delay Bottlenecks\n• CI/CD Dashboards-as-Code\n  (GitHub Actions sync)")
]

col_w = Inches(2.2)
col_gap = Inches(0.18)
start_x = Inches(0.8)
start_y = Inches(1.8)

for i, (step_title, step_desc) in enumerate(flow_steps):
    x = start_x + i * (col_w + col_gap)
    hdr_box = s2.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, start_y, col_w, Inches(0.6))
    hdr_box.fill.solid()
    hdr_box.fill.fore_color.rgb = DARK_BLUE
    hdr_box.line.color.rgb = DARK_BLUE
    p = hdr_box.text_frame.paragraphs[0]
    p.text = step_title
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = WHITE
    p.alignment = PP_ALIGN.CENTER

    cnt_box = s2.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, start_y + Inches(0.65), col_w, Inches(4.5))
    cnt_box.fill.solid()
    cnt_box.fill.fore_color.rgb = LIGHT_BG
    cnt_box.line.color.rgb = BORDER_COL
    tf = cnt_box.text_frame
    tf.word_wrap = True
    p_body = tf.paragraphs[0]
    p_body.text = step_desc
    p_body.font.size = Pt(11)
    p_body.font.color.rgb = CHARCOAL

# ==========================================
# SLIDE 3: Key Technical Terms Explained
# ==========================================
s3 = prs.slides.add_slide(blank_slide_layout)
add_header(s3, "Core Technical Terms & Concepts", "Data Engineering Vocabulary")

terms = [
    ("Medallion Architecture (Bronze / Silver / Gold)",
     "Data organization pattern in modern Lakehouses:\n• Bronze: Raw, unaltered incoming data logs directly from stream.\n• Silver: Cleaned, schema-enforced, deduplicated columnar Parquet.\n• Gold: Business-level aggregated metrics and scorecards ready for dashboards."),
    ("Apache Kafka (KRaft Mode)",
     "High-throughput distributed event-streaming platform. We run Kafka without Zookeeper using modern KRaft (Kafka Raft Metadata) mode to reliably buffer fast incoming vehicle telemetry without drops."),
    ("Apache PySpark (Distributed Compute)",
     "Distributed data-processing engine. PySpark computes multi-dimensional aggregations (speeds, vehicle state, delays) across large datasets much faster than single-node Python."),
    ("KubernetesExecutor (Apache Airflow)",
     "Dynamic worker architecture. Instead of maintaining static worker servers, Airflow launches a dedicated, isolated Kubernetes pod on-demand for every individual pipeline task and destroys it on completion."),
    ("Serving Layer vs. Data Lakehouse Storage",
     "S3 stores voluminous raw and transformed datasets (GBs/TBs) cheaply. PostgreSQL acts as the serving layer storing only lightweight KPI summary scorecards (KBs) for high-speed sub-second dashboard rendering.")
]

y_pos = Inches(1.6)
for title, desc in terms:
    card = s3.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), y_pos, Inches(11.733), Inches(1.0))
    card.fill.solid()
    card.fill.fore_color.rgb = LIGHT_BG
    card.line.color.rgb = BORDER_COL
    
    tf = card.text_frame
    tf.word_wrap = True
    p_title = tf.paragraphs[0]
    p_title.text = title
    p_title.font.size = Pt(13)
    p_title.font.bold = True
    p_title.font.color.rgb = DARK_BLUE
    
    p_desc = tf.add_paragraph()
    p_desc.text = desc
    p_desc.font.size = Pt(10.5)
    p_desc.font.color.rgb = CHARCOAL
    p_desc.space_before = Pt(3)
    
    y_pos += Inches(1.1)

# ==========================================
# SLIDE 4: Python Codebase & Responsibilities
# ==========================================
s4 = prs.slides.add_slide(blank_slide_layout)
add_header(s4, "Python Codebase & Core Components", "Implementation Modules")

files = [
    ("streaming/producer.py", "Kafka Real-Time Producer",
     "• Ingests live vehicle data from MBTA API (https://api-v3.mbta.com/vehicles?include=trip,stop).\n• Extracts vehicle coordinates, speeds, status, trip details, and bearing.\n• Serializes payloads into JSON and streams them to Kafka topic 'transit-vehicles'."),
    
    ("streaming/consumer.py", "Dual-Mode Kafka Sinks",
     "• Subscribes to 'transit-vehicles' topic with group 'cde-bronze-group'.\n• Micro-batch buffering: writes raw events to local JSONL files.\n• Dual-Mode Sync: In 'cloud' mode (boto3), automatically syncs micro-batches to AWS S3 Bronze partition (s3://cde-transit-lakehouse.../bronze/date=YYYY-MM-DD/) every 50 records or 60s."),
    
    ("transformation/to_silver.py", "Bronze to Silver PySpark Job",
     "• Reads raw Bronze JSONL files from AWS S3.\n• Enforces typed schema (timestamps, floats, integers, route mappings).\n• Deduplicates records on (vehicle_id, updated_at).\n• Writes Snappy-compressed columnar Parquet partitioned by date into S3 Silver bucket."),
    
    ("transformation/to_gold.py", "Silver to Gold Analytics Job",
     "• Reads Silver Parquet dataset using PySpark.\n• KPI 1 (route_performance): Computes average speed (mph), ping volume, moving count, stopped count per route.\n• KPI 2 (congestion_hotspots): Calculates stop-level speeds and stuck vehicle counts.\n• Writes partitioned Gold Parquet tables to S3 and syncs KPIs to PostgreSQL serving DB.")
]

y_pos = Inches(1.6)
for path, name, desc in files:
    card = s4.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), y_pos, Inches(11.733), Inches(1.25))
    card.fill.solid()
    card.fill.fore_color.rgb = LIGHT_BG
    card.line.color.rgb = BORDER_COL
    
    tf = card.text_frame
    tf.word_wrap = True
    p1 = tf.paragraphs[0]
    p1.text = f"{path}  —  {name}"
    p1.font.size = Pt(13)
    p1.font.bold = True
    p1.font.color.rgb = DARK_BLUE
    
    p2 = tf.add_paragraph()
    p2.text = desc
    p2.font.size = Pt(10.5)
    p2.font.color.rgb = CHARCOAL
    p2.space_before = Pt(3)
    
    y_pos += Inches(1.35)

# ==========================================
# SLIDE 5: Pipeline Orchestration & Cloud Infrastructure
# ==========================================
s5 = prs.slides.add_slide(blank_slide_layout)
add_header(s5, "Pipeline Orchestration & Cloud Infrastructure", "Airflow & Kubernetes")

card_left = s5.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.3))
card_left.fill.solid()
card_left.fill.fore_color.rgb = LIGHT_BG
card_left.line.color.rgb = BORDER_COL
tf_l = card_left.text_frame
tf_l.word_wrap = True

p_l1 = tf_l.paragraphs[0]
p_l1.text = "Airflow DAG: mbta_transit_lakehouse_pipeline"
p_l1.font.size = Pt(14)
p_l1.font.bold = True
p_l1.font.color.rgb = DARK_BLUE

p_l2 = tf_l.add_paragraph()
p_l2.text = ("Orchestrated via orchestration/dags/transit_pipeline_dag.py on a daily schedule (@daily):\n\n"
             "• Task 1: check_bronze_data_available\n"
             "  Validates incoming Bronze stream files in AWS S3 or local lakehouse.\n\n"
             "• Task 2: run_pyspark_silver_transformation\n"
             "  Triggers PySpark to clean, deduplicate, and partition data into Silver Parquet.\n\n"
             "• Task 3: run_pyspark_gold_aggregations\n"
             "  Computes route speeds, congestion hotspots, and updates PostgreSQL serving tables.\n\n"
             "• Task 4: pipeline_complete\n"
             "  Execution barrier & success notification confirming SLA attainment.")
p_l2.font.size = Pt(11)
p_l2.font.color.rgb = CHARCOAL
p_l2.space_before = Pt(10)

card_right = s5.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(6.933), Inches(1.6), Inches(5.6), Inches(5.3))
card_right.fill.solid()
card_right.fill.fore_color.rgb = LIGHT_BG
card_right.line.color.rgb = BORDER_COL
tf_r = card_right.text_frame
tf_r.word_wrap = True

p_r1 = tf_r.paragraphs[0]
p_r1.text = "Cloud Infrastructure & Containerization"
p_r1.font.size = Pt(14)
p_r1.font.bold = True
p_r1.font.color.rgb = DARK_BLUE

p_r2 = tf_r.add_paragraph()
p_r2.text = ("• Kubernetes (Kind Cluster): 'cde-cluster' with worker and control-plane nodes.\n\n"
             "• Helm Deployment: Official Apache Airflow 2.10.3 Helm Chart configured with KubernetesExecutor.\n\n"
             "• Custom Docker Image: ghcr.io/dipk6545/cde-airflow:latest packaged with Java 17, Apache PySpark 3.5.3, AWS CLI, and PostgreSQL drivers.\n\n"
             "• Prisma Cloud Serverless PostgreSQL: Production metadata backend (50 Airflow tables) + business KPI serving layer.\n\n"
             "• AWS S3 (Sydney ap-southeast-2): Scalable Medallion Lakehouse object storage.\n\n"
             "• GitHub Codespaces Automation: Automated single-command startup script (scripts/start_codespaces.sh).")
p_r2.font.size = Pt(11)
p_r2.font.color.rgb = CHARCOAL
p_r2.space_before = Pt(10)

# ==========================================
# SLIDE 6: Grafana Transit Observability Dashboard
# ==========================================
s6 = prs.slides.add_slide(blank_slide_layout)
add_header(s6, "Executive Transit Control Dashboard", "Grafana Observability")

panels = [
    ("Panel 1: Total GPS Pings Analyzed", "Volume Metric",
     "Displays total raw GPS position updates processed (e.g. 2,405 pings). Demonstrates ingestion pipeline throughput and data availability."),
    
    ("Panel 2: Fleet Average Speed", "Network Health Metric",
     "Single weighted KPI tracking overall average fleet velocity (e.g. 12.1 mph). Drops below 10 mph alert operators of city-wide traffic congestion."),
    
    ("Panel 3: Average Vehicle Speed by Route", "Corridor Performance",
     "Horizontal bar chart ranking routes from fastest to slowest. Highlights rapid transit lines (Route 39 @ 16.1 mph) vs. delayed lines (Route 111 @ 7.9 mph)."),
    
    ("Panel 4: Live Fleet Status (Moving vs Stopped)", "Fleet Utilization",
     "Donut visualization showing active transit proportion: ~59% vehicles moving vs. ~41% stopped at red lights, passenger boardings, or stations."),
    
    ("Panel 5: Top 5 Transit Bottlenecks & Hotspots", "Infrastructure Planning",
     "Tabular root-cause analysis showing specific bus stops with severe delays (e.g. Sullivan Square @ 2.1 mph, 115 stuck vehicle pings).")
]

y_pos = Inches(1.6)
for title, cat, desc in panels:
    card = s6.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), y_pos, Inches(11.733), Inches(0.95))
    card.fill.solid()
    card.fill.fore_color.rgb = LIGHT_BG
    card.line.color.rgb = BORDER_COL
    
    tf = card.text_frame
    tf.word_wrap = True
    p1 = tf.paragraphs[0]
    p1.text = f"{title}  [{cat}]"
    p1.font.size = Pt(12.5)
    p1.font.bold = True
    p1.font.color.rgb = DARK_BLUE
    
    p2 = tf.add_paragraph()
    p2.text = desc
    p2.font.size = Pt(10.5)
    p2.font.color.rgb = CHARCOAL
    p2.space_before = Pt(2)
    
    y_pos += Inches(1.05)

# ==========================================
# SLIDE 7: Dashboards-as-Code & Dynamic Time Filtering
# ==========================================
s7 = prs.slides.add_slide(blank_slide_layout)
add_header(s7, "CI/CD Dashboards-as-Code & Dynamic Filtering", "Engineering Best Practices")

card_l7 = s7.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.3))
card_l7.fill.solid()
card_l7.fill.fore_color.rgb = LIGHT_BG
card_l7.line.color.rgb = BORDER_COL
tf_l7 = card_l7.text_frame
tf_l7.word_wrap = True

p_l7_1 = tf_l7.paragraphs[0]
p_l7_1.text = "Automated Sync (Dashboards-as-Code)"
p_l7_1.font.size = Pt(14)
p_l7_1.font.bold = True
p_l7_1.font.color.rgb = DARK_BLUE

p_l7_2 = tf_l7.add_paragraph()
p_l7_2.text = ("• Problem: Manual JSON copy-pasting into Grafana UI causes configuration drift and human errors.\n\n"
               "• Solution: Modern CI/CD workflow (.github/workflows/sync-grafana.yml).\n\n"
               "• Implementation:\n"
               "  - Dashboard JSON defined in git (orchestration/grafana_transit_dashboard.json).\n"
               "  - Grafana Service Account Token secured in GitHub Secrets (GRAFANA_API_KEY).\n"
               "  - On every git push to main, GitHub Actions calls Grafana Cloud REST API (/api/dashboards/db) to overwrite and live-update panels in < 8 seconds.")
p_l7_2.font.size = Pt(11)
p_l7_2.font.color.rgb = CHARCOAL
p_l7_2.space_before = Pt(10)

card_r7 = s7.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(6.933), Inches(1.6), Inches(5.6), Inches(5.3))
card_r7.fill.solid()
card_r7.fill.fore_color.rgb = LIGHT_BG
card_r7.line.color.rgb = BORDER_COL
tf_r7 = card_r7.text_frame
tf_r7.word_wrap = True

p_r7_1 = tf_r7.paragraphs[0]
p_r7_1.text = "Dynamic Time Windowing & Data Efficiency"
p_r7_1.font.size = Pt(14)
p_r7_1.font.bold = True
p_r7_1.font.color.rgb = DARK_BLUE

p_r7_2 = tf_r7.add_paragraph()
p_r7_2.text = ("• Dynamic Filtering: Implemented Grafana $__timeFilter(updated_at) SQL macros across all panels so users can freely switch between 5 minutes, 24 hours, or 7 days.\n\n"
               "• Null Handling: Utilized SQL COALESCE(..., 0) so empty micro-windows display 0 instead of error states.\n\n"
               "• Storage Efficiency (Serving vs Lakehouse):\n"
               "  - Millions of raw events stay in low-cost AWS S3.\n"
               "  - PostgreSQL only saves ~5 summary rows per run (< 1 MB per year), guaranteeing the database never exhausts storage.")
p_r7_2.font.size = Pt(11)
p_r7_2.font.color.rgb = CHARCOAL
p_r7_2.space_before = Pt(10)

# ==========================================
# SLIDE 8: Project Summary & Business Impact
# ==========================================
s8 = prs.slides.add_slide(blank_slide_layout)
add_header(s8, "Project Summary & Engineering Impact", "Conclusion")

card_summary = s8.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.8), Inches(1.6), Inches(11.733), Inches(5.3))
card_summary.fill.solid()
card_summary.fill.fore_color.rgb = LIGHT_BG
card_summary.line.color.rgb = BORDER_COL
tf_s = card_summary.text_frame
tf_s.word_wrap = True

p_s1 = tf_s.paragraphs[0]
p_s1.text = "Key Achievements & Value Delivered"
p_s1.font.size = Pt(16)
p_s1.font.bold = True
p_s1.font.color.rgb = DARK_BLUE

summary_points = [
    ("End-to-End Scalability", "Built a complete modern data stack streaming live MBTA transit vehicle positions through Kafka, persisting to an AWS S3 Medallion Lakehouse, and computing KPIs with PySpark."),
    ("Resilient Kubernetes Orchestration", "Deployed Apache Airflow 2.10.3 on Kubernetes (Kind) using the KubernetesExecutor, ensuring isolated task pods, automated retries, and reproducible Cloud environments."),
    ("Production Serving Separation", "Decoupled analytical storage (AWS S3 Sydney) from operational serving (Prisma Cloud PostgreSQL), ensuring sub-second Grafana query performance without overloading the database."),
    ("Automated Dashboards-as-Code", "Integrated GitHub Actions CI/CD with Grafana Cloud API, automating dashboard deployments directly from version control."),
    ("Actionable Civic Intelligence", "Empowers transit operations with real-time fleet health visibility and pinpoints exact delay hotspots for urban transit intervention.")
]

for title, body in summary_points:
    p_item = tf_s.add_paragraph()
    p_item.text = f"• {title}: {body}"
    p_item.font.size = Pt(11.5)
    p_item.font.color.rgb = CHARCOAL
    p_item.space_before = Pt(8)

output_path = "/mnt/HDD/MDEP-CDE Project/CDE/Transit_Lakehouse_Presentation.pptx"
prs.save(output_path)
print(f"Successfully generated clean 8-slide presentation at {output_path}")
