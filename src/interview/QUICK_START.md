# 🚀 Quick Start Guide

## Fastest Way to Start

```bash
cd src/interview
./run_practice.sh
```

That's it! Choose your practice system from the menu.

---

## Command Line Options

```bash
# Interactive menu
./run_practice.sh

# Direct launch specific practice
./run_practice.sh batch       # Batch ETL (CSV → Parquet)
./run_practice.sh kafka       # Kafka Streaming (Kafka → Kafka)
./run_practice.sh parquet     # Data Lake (Kafka → Parquet)
./run_practice.sh launcher    # Master launcher (all-in-one)

# Help
./run_practice.sh help
```

---

## Alternative: Python Direct

```bash
# Master launcher (recommended)
python practice_launcher.py

# Individual practice systems
python batch_etl_practice.py
python kafka_streaming_practice.py
python kafka_to_parquet_practice.py
```

---

## For Kafka Practice (Streaming Systems)

### Start Kafka First

```bash
# Start Kafka infrastructure
docker-compose up -d

# Verify running
docker ps

# Access Kafka UI
open http://localhost:8080
```

### Generate Test Data

```bash
# Continuous orders
python kafka_producer_orders.py

# Burst mode
python kafka_producer_orders.py --mode burst
```

---

## Learning Path

**Week 1**: Batch ETL
```bash
./run_practice.sh batch
```
- Master fundamentals
- Target: < 15 minutes

**Week 2**: Kafka Streaming
```bash
./run_practice.sh kafka
```
- Real-time processing
- Target: < 20 minutes

**Week 3**: Data Lake Ingestion
```bash
./run_practice.sh parquet
```
- Streaming + Persistence
- Target: < 25 minutes

---

## Practice Modes

Each system has 4 modes:

1. **Guided** - Step-by-step with hints (learning)
2. **Timed** - Speed challenge with timer (practice)
3. **Interview** - No hints, time limit (simulation)
4. **Reference** - Study complete solution

---

## Files Created

Your practice code is saved to:
```
/tmp/etl_practice/
├── batch/          # Batch ETL practice files
├── kafka_stream/   # Kafka streaming practice files
└── kafka_parquet/  # Data lake ingestion practice files
```

---

## Need Help?

- **Setup**: See [KAFKA_ETL_README.md](KAFKA_ETL_README.md) for Kafka setup
- **Full Guide**: See [README.md](README.md) for complete documentation
- **Issues**: Check Kafka is running with `docker ps`

---

## Success Metrics

| Level | Time | Status |
|-------|------|--------|
| Beginner | 30+ min | Learning ⭕ |
| Intermediate | 15-20 min | Practicing 🟡 |
| Advanced | 10-15 min | Mastering 🟠 |
| Expert | < 10 min | Interview-ready ✅ |

Start practicing now! 🚀
