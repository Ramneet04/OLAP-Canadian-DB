"""
STEP 1: Data Quality Report
Shows current state of off_v2.duckdb
Run: python step1_quality_report.py
"""
import duckdb
from pathlib import Path

def resolve_db_path() -> str:
    data_dir = Path(__file__).resolve().parents[1] / "data"
    preferred = data_dir / "canada_olap.duckdb"
    if preferred.exists():
        return str(preferred)
    candidates = sorted(data_dir.glob("*.duckdb"))
    if not candidates:
        raise FileNotFoundError(f"No .duckdb file found in {data_dir}")
    return str(candidates[0])

con = duckdb.connect(resolve_db_path(), read_only=True)
total = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]

print("=" * 60)
print("OFF CANADA OLAP - DATA QUALITY REPORT")
print("=" * 60)
print(f"\nTotal products: {total:,}")

# Country breakdown
print("\n--- PRODUCTS BY COUNTRY ---")
countries = con.execute("""
    SELECT primary_country, COUNT(*) as count
    FROM products
    GROUP BY primary_country
    ORDER BY count DESC
    LIMIT 10
""").fetchdf()
for _, r in countries.iterrows():
    print(f"  {r['primary_country']:<20} {r['count']:>8,}")

# Nutrition completeness
print("\n--- NUTRITION COMPLETENESS ---")
nutrition_cols = [
    'energy_kcal_100g', 'proteins_100g', 'fat_100g',
    'carbohydrates_100g', 'sugars_100g', 'sodium_100g',
    'fiber_100g', 'salt_100g'
]
for col in nutrition_cols:
    count = con.execute(f"""
        SELECT COUNT(*) FROM products 
        WHERE {col} IS NOT NULL
    """).fetchone()[0]
    pct = count/total*100
    bar = "█" * int(pct/5)
    print(f"  {col:<25} {pct:>5.1f}% {bar}")

# Key fields completeness
print("\n--- KEY FIELDS COMPLETENESS ---")
key_cols = [
    'product_name', 'brands', 'categories_tags',
    'nutriscore_grade', 'nova_group', 'ecoscore_grade',
    'image_url', 'ingredients_text', 'labels_tags'
]
for col in key_cols:
    count = con.execute(f"""
        SELECT COUNT(*) FROM products 
        WHERE {col} IS NOT NULL 
        AND CAST({col} AS VARCHAR) NOT IN ('', '[]', 'unknown')
    """).fetchone()[0]
    pct = count/total*100
    bar = "█" * int(pct/5)
    print(f"  {col:<25} {pct:>5.1f}% {bar}")

# Duplicates detection
print("\n--- DUPLICATE DETECTION ---")
dupes = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT brands, product_name, COUNT(*) as cnt
        FROM products
        WHERE brands IS NOT NULL 
        AND product_name IS NOT NULL
        GROUP BY brands, product_name
        HAVING COUNT(*) > 1
    )
""").fetchone()[0]

dupes_products = con.execute("""
    SELECT COALESCE(SUM(cnt - 1), 0) FROM (
        SELECT brands, product_name, COUNT(*) as cnt
        FROM products
        WHERE brands IS NOT NULL 
        AND product_name IS NOT NULL
        GROUP BY brands, product_name
        HAVING COUNT(*) > 1
    )
""").fetchone()[0]

print(f"  Duplicate groups:    {dupes:>8,}")
print(f"  Redundant records:   {int(dupes_products):>8,}")
print(f"  Dedup potential:     {int(dupes_products)/total*100:.1f}% reduction possible")

# Nutriscore breakdown
print("\n--- NUTRISCORE DISTRIBUTION ---")
ns = con.execute("""
    SELECT nutriscore_grade, COUNT(*) as count
    FROM products
    WHERE nutriscore_grade IS NOT NULL
    AND nutriscore_grade != 'unknown'
    GROUP BY nutriscore_grade
    ORDER BY nutriscore_grade
""").fetchdf()
for _, r in ns.iterrows():
    pct = r['count']/total*100
    print(f"  {r['nutriscore_grade'].upper()}: {r['count']:>8,} ({pct:.1f}%)")

unknown = con.execute("""
    SELECT COUNT(*) FROM products 
    WHERE nutriscore_grade = 'unknown' 
    OR nutriscore_grade IS NULL
""").fetchone()[0]
print(f"  Unknown/NULL: {unknown:>8,} ({unknown/total*100:.1f}%)")

print("\n" + "=" * 60)
print("SUMMARY + IMPROVEMENT PLAN")
print("=" * 60)
complete = con.execute("""
    SELECT COUNT(*) FROM products 
    WHERE energy_kcal_100g IS NOT NULL 
    AND proteins_100g IS NOT NULL 
    AND sodium_100g IS NOT NULL
""").fetchone()[0]
print(f"  Total products:            {total:,}")
print(f"  Complete nutrition:        {complete:,} ({complete/total*100:.1f}%)")
print(f"  Potential duplicates:      {int(dupes_products):,}")
print(f"  Missing nutriscore:        {unknown:,} ({unknown/total*100:.1f}%)")
print(f"\n  IMPROVEMENTS PLANNED:")
print(f"  → FoodData Central fills nutrition gaps")
print(f"  → product_group_id eliminates {int(dupes_products):,} duplicates")
print(f"  → Robotoff API predicts missing nutriscore")
print(f"  → Canada Food Guide adds recipe relationships")

con.close()