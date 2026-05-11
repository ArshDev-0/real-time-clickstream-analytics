from kafka import KafkaConsumer
import json
from collections import Counter
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import time
from datetime import datetime

# ---------------- KAFKA SETUP ----------------
consumer = KafkaConsumer(
    "clickstream-topic",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ---------------- DATA STRUCTURES ----------------
page_counter = Counter()
country_counter = Counter()
users = set()

traffic_timestamps = []
traffic_counts = []
unique_user_counts = []
current_second = int(time.time())
visit_count_current_second = 0

# ---------------- PLOTTING SETUP ----------------
plt.style.use("dark_background")
plt.ion()

# Create single dashboard window using GridSpec
fig = plt.figure(figsize=(14, 12))
gs = gridspec.GridSpec(3, 2, figure=fig)

ax_bar = fig.add_subplot(gs[0, 0])
ax_pie = fig.add_subplot(gs[0, 1])
ax_line1 = fig.add_subplot(gs[1, :])  # full width
ax_line2 = fig.add_subplot(gs[2, :])  # full width

fig.suptitle("REAL-TIME CLICKSTREAM ANALYTICS DASHBOARD", fontsize=16)

# ---------------- TERMINAL REFRESH CONTROL ----------------
last_update_time = time.time()
update_interval = 0.5
def smooth_clear():
    print("\033[H\033[J", end="")

print("Consumer started... Waiting for data...\n")

# ---------------- MAIN LOOP ----------------
for message in consumer:
    data = message.value

    page_counter[data["page"]] += 1
    country_counter[data["country"]] += 1
    users.add(data["user_id"])

    total_visits = sum(page_counter.values())

    # -------- TRAFFIC PER SECOND --------
    now_second = int(time.time())

    if now_second == current_second:
        visit_count_current_second += 1
    else:
        traffic_timestamps.append(
            datetime.fromtimestamp(current_second).strftime("%H:%M:%S")
        )
        traffic_counts.append(visit_count_current_second)
        unique_user_counts.append(len(users))

        current_second = now_second
        visit_count_current_second = 1

        # Keep last 20 seconds for clean view
        traffic_timestamps = traffic_timestamps[-20:]
        traffic_counts = traffic_counts[-20:]
        unique_user_counts = unique_user_counts[-20:]

    current_time = time.time()

    # -------- TERMINAL DASHBOARD --------
    if current_time - last_update_time > update_interval:
        last_update_time = current_time

        smooth_clear()
        print("=" * 60)
        print("       REAL-TIME CLICKSTREAM MONITOR")
        print("=" * 60)
        print(f" Total Visits  : {total_visits}")
        print(f" Unique Users  : {len(users)}")
        print("-" * 60)
        print(" Top Pages:")
        for page, count in page_counter.most_common(3):
            print(f"   {page:<15} {count}")
        print("-" * 60)
        print(" Top Countries:")
        for country, count in country_counter.most_common(3):
            print(f"   {country:<15} {count}")
        print("=" * 60)

    # ================= DASHBOARD =================

    # -------- TOP 5 PAGES BAR --------
    ax_bar.clear()
    top_pages = page_counter.most_common(5)
    pages = [p[0] for p in top_pages]
    counts = [p[1] for p in top_pages]

    bars = ax_bar.bar(pages, counts, edgecolor='white')
    ax_bar.set_title("Top 5 Pages")
    ax_bar.set_ylabel("Visits")
    ax_bar.tick_params(axis='x', rotation=30)
    ax_bar.grid(axis='y', linestyle='--', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax_bar.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f'{int(height)}',
            ha='center',
            va='bottom',
            fontsize=9
        )

    # -------- COUNTRY DISTRIBUTION PIE --------
    ax_pie.clear()
    top_countries = country_counter.most_common(5)
    countries = [c[0] for c in top_countries]
    country_counts = [c[1] for c in top_countries]

    if country_counts:
        ax_pie.pie(
            country_counts,
            labels=countries,
            autopct='%1.1f%%',
            startangle=90
        )

    ax_pie.set_title("Country Distribution")

    # -------- TRAFFIC TREND LINE --------
    ax_line1.clear()
    ax_line1.plot(traffic_timestamps, traffic_counts)
    ax_line1.set_title("Real-Time Traffic (Visits per Second)")
    ax_line1.set_ylabel("Visits")
    ax_line1.tick_params(axis='x', rotation=45)
    ax_line1.grid(alpha=0.3)

    # -------- UNIQUE USERS GROWTH --------
    ax_line2.clear()
    ax_line2.plot(traffic_timestamps, unique_user_counts)
    ax_line2.set_title("Unique Users Growth")
    ax_line2.set_ylabel("Total Unique Users")
    ax_line2.tick_params(axis='x', rotation=45)
    ax_line2.grid(alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.canvas.draw()
    fig.canvas.flush_events()
