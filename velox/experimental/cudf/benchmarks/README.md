# CuDF Benchmarks

Benchmark binaries for TPC-H and TPC-DS queries with optional CuDF GPU acceleration.

## Binaries

| Binary | Benchmark | Source |
|--------|-----------|--------|
| `velox_cudf_tpch_benchmark` | TPC-H (Q1-Q22) | `CudfTpchBenchmark.cpp` |
| `velox_cudf_tpcds_benchmark` | TPC-DS (Q1-Q99) | `CudfTpcdsBenchmark.cpp` |

Both binaries support CPU-only (Hive) and GPU-accelerated (CuDF) modes.

## Build

```bash
CUDA_ARCHITECTURES="native" EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_BENCHMARKS=ON" make cudf
cd _build/release
ninja velox_cudf_tpch_benchmark velox_cudf_tpcds_benchmark
```

---

## TPC-H Benchmark

### Data

Generate TPC-H parquet data using the [velox-testing](https://github.com/rapidsai/velox-testing) data generation tool (see [Data Generation](#data-generation) below), or the standard `dbgen` tool and convert decimal columns to float.

### Run

```bash
# CPU (Hive) - all queries
./velox_cudf_tpch_benchmark --data_path=/path/to/tpch/sf100 --data_format=parquet

# GPU (CuDF) - all queries
./velox_cudf_tpch_benchmark --data_path=/path/to/tpch/sf100 --data_format=parquet \
  --velox_cudf_table_scan=true
```

---

## TPC-DS Benchmark

TPC-DS plans are loaded from pre-dumped Velox plan JSON files (serialized from Presto).

### 1. Get Plan JSON Files

Clone the plans repository:

```bash
git clone https://github.com/karthikeyann/VeloxPlans.git
# Plans are at: VeloxPlans/presto/tpcds/sf100/
```

The directory contains `Q1.json`, `Q2.json`, ..., `Q99.json`.

### 2. Get TPC-DS Data

Generate TPC-DS parquet data using the [velox-testing](https://github.com/rapidsai/velox-testing) data generation tool (see [Data Generation](#data-generation) below). The data directory must have one subdirectory per table:

```
/path/to/tpcds/sf100/
  store_sales/
  customer/
  date_dim/
  item/
  ...
```

Each subdirectory contains parquet files for that table.

### 3. Run

**CPU (Hive) - all queries (folly benchmark mode):**

```bash
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=/path/to/VeloxPlans/presto/tpcds/sf100 \
  --data_format=parquet
```

**CPU (Hive) - single query with stats:**

```bash
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=/path/to/VeloxPlans/presto/tpcds/sf100 \
  --data_format=parquet \
  --run_query_verbose=1
```

**GPU (CuDF) - all queries:**

```bash
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=/path/to/VeloxPlans/presto/tpcds/sf100 \
  --data_format=parquet \
  --cudf_enabled
```

**GPU (CuDF) - single query with stats:**

```bash
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=/path/to/VeloxPlans/presto/tpcds/sf100 \
  --data_format=parquet \
  --cudf_enabled \
  --run_query_verbose=1
```

### TPC-DS Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data_path` | (required) | Root directory of TPC-DS table data |
| `--plan_path` | (required) | Directory containing Q*.json plan files |
| `--data_format` | `parquet` | Data file format |
| `--run_query_verbose` | `-1` | Run single query with stats (`-1` = run all) |
| `--num_drivers` | `4` | Number of parallel drivers |
| `--include_results` | `false` | Print query results |

### CuDF-specific Flags (with `--cudf_enabled`)

| Flag | Default | Description |
|------|---------|-------------|
| `--cudf_enabled` | `false` | Enable CuDF GPU acceleration |
| `--cudf_chunk_read_limit` | `0` | Chunk read limit for cuDF parquet reader |
| `--cudf_pass_read_limit` | `0` | Pass read limit for cuDF parquet reader |
| `--cudf_gpu_batch_size_rows` | `100000` | GPU batch size in rows |
| `--cudf_memory_resource` | `async` | RMM memory resource type |
| `--cudf_memory_percent` | `50` | Percentage of GPU memory to allocate for pool memory resource only |
| `--velox_cudf_table_scan` | `true` | Use CuDF table scan |
| `--cudf_debug_enabled` | `false` | Enable debug printing |

---

## Data Generation

Both TPC-H and TPC-DS parquet data can be generated using the
[velox-testing](https://github.com/rapidsai/velox-testing) repository.
Full instructions are also available in the
[VeloxPlans TPC-DS README](https://github.com/karthikeyann/VeloxPlans/tree/main/presto/tpcds/sf100).

### Quick Start

```bash
# 1. Clone velox-testing
git clone https://github.com/rapidsai/velox-testing.git
cd velox-testing

# 2. Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r benchmark_data_tools/requirements.txt

# 3. Generate TPC-DS data (sf100)
python benchmark_data_tools/generate_data_files.py \
  --benchmark-type tpcds \
  --data-dir-path /path/to/tpcds/sf100/data \
  --scale-factor 100 \
  --convert-decimals-to-floats

# 4. Generate TPC-H data (sf100)
python benchmark_data_tools/generate_data_files.py \
  --benchmark-type tpch \
  --data-dir-path /path/to/tpch/sf100/data \
  --scale-factor 100 \
  --convert-decimals-to-floats
```

### Key Flags

| Flag | Description |
|------|-------------|
| `--benchmark-type` | `tpcds` or `tpch` |
| `--data-dir-path` | Output directory for parquet files |
| `--scale-factor` | Scale factor (e.g. `1`, `10`, `100`) |
| `--convert-decimals-to-floats` | Convert decimal columns to double (recommended for Velox) |

The output directory will contain one subdirectory per table, each with `.parquet` files.
For a quick sanity check, use `--scale-factor 1` first.

---

## Synthetic NLJ Benchmark Plans

TPC-DS queries only exercise `NestedLoopJoinNode` trivially — all NLJ nodes are
conditionless cross-joins with 1-row build sides. To benchmark the GPU
`CudfNestedLoopJoin` operator with non-trivial workloads, we generate synthetic
query plans that create **inner joins with real join conditions** using existing
TPC-DS parquet data.

### Generate Plans

```bash
python3 velox/experimental/cudf/benchmarks/generate_nlj_plans.py \
  --output_dir=VeloxPlans/synthetic/nlj
```

To regenerate a single query: `--queries=4`

### Run

```bash
# Single query (GPU)
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=VeloxPlans/synthetic/nlj \
  --cudf_enabled=true \
  --run_query_verbose=4

# All queries
for q in 1 2 3 4 5 6 7 8 9 10; do
  echo "=== Q$q ==="
  ./velox_cudf_tpcds_benchmark \
    --data_path=/path/to/tpcds/sf100 \
    --plan_path=VeloxPlans/synthetic/nlj \
    --cudf_enabled=true \
    --run_query_verbose=$q
done

# CPU-only comparison
./velox_cudf_tpcds_benchmark \
  --data_path=/path/to/tpcds/sf100 \
  --plan_path=VeloxPlans/synthetic/nlj \
  --cudf_enabled=false \
  --run_query_verbose=4
```

### SF100 Results

| Q# | Description | Probe | Build | Join Condition | Output | Time | Status |
|----|-------------|-------|-------|----------------|--------|------|--------|
| 1 | store_sales × item (range join) | 288M | 204K | `ss_list_price BETWEEN (i_current_price - 1.0) AND (i_current_price + 1.0)` | — | — | Host OOM |
| 2 | store_sales × date_dim (inequality) | 288M | 365 | `ss_sold_date_sk > d_date_sk` (build filtered: `d_year = 2000`) | — | — | Host OOM |
| 3 | catalog_sales × item (multi-condition) | 144M | 204K | `cs_list_price > i_current_price AND cs_wholesale_cost < i_wholesale_cost` | — | — | Host OOM |
| 4 | store × item (baseline) | 402 | 204K | `i_current_price > 50.0` | 4.6M rows | 324ms | Pass |
| 5 | customer × customer_address | 2M | 50K | `c_current_addr_sk > ca_address_sk` (probe filtered: `c_birth_year > 1970`) | — | — | GPU OOM |
| 6 | web_sales × store_sales (filtered) | 72M | ~30K | `ws_ext_sales_price > ss_ext_sales_price` (build filtered: `ss_store_sk = 1`) | — | — | GPU OOM |
| 7 | store × promotion (date range) | 402 | 1K | `p_start_date_sk BETWEEN (s_closed_date_sk - 100) AND (s_closed_date_sk + 100)` | 6.7K rows | 186ms | Pass |
| 8 | date_dim × store (inequality) | 366 | 402 | `d_date_sk > s_store_sk` (probe filtered: `d_year = 2000`) | 147K rows | 150ms | Pass |
| 9 | web_page × catalog_page (multi-AND) | 2K | 20K | `wp_web_page_sk > cp_catalog_page_sk AND wp_char_count > cp_catalog_page_number` | 2.0M rows | 166ms | Pass |
| 10 | item × household_demo (BETWEEN+AND) | 20K | 7.2K | `i_current_price BETWEEN 10 AND 50 AND hd_dep_count > 0` (probe filtered: `i_category_id = 1`) | 6.2M rows | 309ms | Pass |

### Plan Structure

Each generated plan follows this tree:

```
PartitionedOutputNode (Gather)
  └── NestedLoopJoinNode (INNER, joinCondition=<expr>)
        ├── [probe]  TableScanNode  (or FilterNode → TableScanNode)
        └── [build]  LocalPartitionNode (Gather)
                       └── ProjectNode
                             └── TableScanNode (or FilterNode → TableScanNode)
```

### Adding New Queries

Edit `generate_nlj_plans.py`:

1. Add a `def query_N()` function following the existing pattern.
2. Add it to the `QUERIES` dict at the bottom.
3. If using a new table, add its full schema to `TABLE_SCHEMAS`.
4. Regenerate and test: `--queries=N` then `--run_query_verbose=N`.

Query IDs must be 1-99 (enforced by `VeloxPlanLoader`).

### Serialization Gotchas

- **`LocalPartitionNode`** requires `"type": "GATHER"` — omitting it causes `couldn't find key type`.
- **`DATE` columns** must use `{"name": "DateType", "type": "DATE"}`, not `{"name": "Type", ...}`.
- **`AND`/`OR`** are special forms — use bare `"and"`/`"or"`, NOT `"presto.default.and"`.
- **`dataColumns`** in `HiveTableHandle` must be the **full table schema**, not just read columns.
- **`between`** uses `"presto.default.between"` (it IS a regular function).
