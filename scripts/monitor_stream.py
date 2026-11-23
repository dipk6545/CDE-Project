#!/usr/bin/env python3
"""
Real-Time Terminal Dashboard for MBTA Transit Kafka Stream
Visualizes live vehicles, speeds, routes, and S3 batch status.
"""
import os
import sys
import time
from datetime import datetime, timezone
import json

# Terminal ANSI colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CLEAR = "\033[H\033[J"

def get_latest_logs(log_path, num_lines=15):
    if not os.path.exists(log_path):
        return []
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
            return [l.strip() for l in lines[-num_lines:] if l.strip()]
    except Exception:
        return []

def main():
    consumer_log = "logs_runtime/consumer.log"
    producer_log = "logs_runtime/producer.log"
    
    # Try looking for local bronze files to get stats
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_file = f"data/bronze/date={today_str}/bronze_stream.jsonl"
    
    frame = 0
    spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    try:
        while True:
            spin = spinner[frame % len(spinner)]
            frame += 1
            
            # Read latest pings from local file if available
            recent_pings = []
            total_records = 0
            if os.path.exists(bronze_file):
                try:
                    with open(bronze_file, "r") as f:
                        lines = f.readlines()
                        total_records = len(lines)
                        for l in lines[-6:]:
                            try:
                                recent_pings.append(json.loads(l))
                            except Exception:
                                pass
                except Exception:
                    pass

            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

            output = []
            output.append(CLEAR)
            output.append(f"{CYAN}{BOLD}╔══════════════════════════════════════════════════════════════════════════════════╗{RESET}")
            output.append(f"{CYAN}{BOLD}║         🚌 MBTA LIVE TRANSIT KAFKA STREAM & S3 BRONZE MONITOR {GREEN}{spin}{CYAN}{BOLD}            ║{RESET}")
            output.append(f"{CYAN}{BOLD}╚══════════════════════════════════════════════════════════════════════════════════╝{RESET}")
            output.append(f"{BOLD}Status:{RESET} {GREEN}STREAMING LIVE{RESET}  |  {BOLD}Time:{RESET} {now_str}  |  {BOLD}Topic:{RESET} {YELLOW}transit-vehicles{RESET}")
            output.append(f"{BOLD}Today's Bronze Sink File:{RESET} {DIM}{bronze_file}{RESET}")
            output.append(f"{BOLD}Records Buffered Today:{RESET} {GREEN}{total_records:,} pings{RESET}")
            output.append(f"{CYAN}──────────────────────────────────────────────────────────────────────────────────{RESET}")

            # Table of live incoming vehicles
            output.append(f"{BOLD}{'VEHICLE':<12} {'ROUTE':<18} {'SPEED':<12} {'STATUS':<16} {'LAT / LON':<20}{RESET}")
            output.append(f"{DIM}{'─'*10}   {'─'*16}   {'─'*10}   {'─'*14}   {'─'*18}{RESET}")

            if recent_pings:
                for p in recent_pings:
                    v_id = str(p.get("vehicle_id", "N/A"))[:10]
                    r_id = str(p.get("route_id", "N/A"))[:16]
                    speed = p.get("speed", 0.0)
                    speed_str = f"{speed:.1f} mph"
                    speed_color = GREEN if speed > 15 else (YELLOW if speed > 5 else RED)
                    status = str(p.get("status", "UNKNOWN"))[:14]
                    lat = p.get("latitude", 0.0)
                    lon = p.get("longitude", 0.0)
                    coords = f"{lat:.4f}, {lon:.4f}"
                    output.append(f"{BOLD}{v_id:<12}{RESET} {r_id:<18} {speed_color}{speed_str:<12}{RESET} {status:<16} {DIM}{coords:<20}{RESET}")
            else:
                output.append(f"{YELLOW}⏳ Waiting for first micro-batch from Kafka broker...{RESET}")

            output.append(f"{CYAN}──────────────────────────────────────────────────────────────────────────────────{RESET}")
            output.append(f"{BOLD}📡 LIVE CONSUMER & S3 CLOUD SYNC ACTIVITY (Latest Logs):{RESET}")

            c_logs = get_latest_logs(consumer_log, 4)
            if c_logs:
                for cl in c_logs:
                    if "Upload" in cl or "S3" in cl:
                        output.append(f"  {GREEN}☁️  {cl}{RESET}")
                    else:
                        output.append(f"  {DIM}•  {cl}{RESET}")
            else:
                output.append(f"  {DIM}(Listening on consumer group 'cde-bronze-group'...){RESET}")

            output.append(f"{CYAN}──────────────────────────────────────────────────────────────────────────────────{RESET}")
            output.append(f"{YELLOW}{BOLD}Press [Ctrl+C] to stop all streaming processes and Kafka{RESET}")

            sys.stdout.write("\n".join(output) + "\n")
            sys.stdout.flush()
            time.sleep(1.5)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}🛑 Stopping all streaming processes (Producer, Consumer & Kafka)...{RESET}")
        os.system("pkill -f 'streaming/producer.py' 2>/dev/null || true")
        os.system("pkill -f 'streaming/consumer.py' 2>/dev/null || true")
        os.system("docker compose -f docker-compose-kafka.yml stop >/dev/null 2>&1 || true")
        print(f"{GREEN}✅ All streaming processes and Kafka container stopped successfully.{RESET}")

if __name__ == "__main__":
    main()
