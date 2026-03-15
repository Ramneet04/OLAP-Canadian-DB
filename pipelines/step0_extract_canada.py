from pathlib import Path
import duckdb

SRC_DB  = str(sorted(Path(__file__).resolve().parents[1].glob("data/*.duckdb"))[0])
DEST_DB = str(Path(__file__).resolve().parents[1] / "data" / "canada_olap.duckdb")

print("=" * 60)
print("STEP 0: Extract Canada-only OLAP Database")
print("=" * 60)
print(f"  Source : {SRC_DB}")
print(f"  Dest   : {DEST_DB}")

src = duckdb.connect(SRC_DB, read_only=True)
total_src = src.execute("SELECT COUNT(*) FROM products").fetchone()[0]
canada_count = src.execute("SELECT COUNT(*) FROM products WHERE primary_country = 'canada'").fetchone()[0]
print(f"\n  Source total rows  : {total_src:,}")
print(f"  Canadian rows      : {canada_count:,} ({canada_count/total_src*100:.1f}%)")
print(f"  Rows to discard    : {total_src - canada_count:,}")

dest = duckdb.connect(DEST_DB)
dest.execute(f"ATTACH '{SRC_DB}' AS src (READ_ONLY)")
dest.execute("""
    CREATE TABLE IF NOT EXISTS products AS
    SELECT * FROM src.products
    WHERE primary_country = 'canada'
""")
dest.execute("DETACH src")

dest.execute("CREATE INDEX IF NOT EXISTS idx_code   ON products (code)")
dest.execute("CREATE INDEX IF NOT EXISTS idx_brands ON products (brands)")

final_count = dest.execute("SELECT COUNT(*) FROM products").fetchone()[0]
print(f"\n  Rows written       : {final_count:,}")

src_size  = Path(SRC_DB).stat().st_size  / 1024 / 1024
dest_size = Path(DEST_DB).stat().st_size / 1024 / 1024
print(f"  Source DB size     : {src_size:.1f} MB")
print(f"  Canada DB size     : {dest_size:.1f} MB")
print(f"  Size reduction     : {100 - dest_size/src_size*100:.1f}%")

print("\n✅ canada_olap.duckdb created successfully")
print(f"   All future pipelines use: data/canada_olap.duckdb")

src.close()
dest.close()
