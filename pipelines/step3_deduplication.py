"""
STEP 3: Product Deduplication with product_group_id
Uses fuzzy matching to link duplicate products
Run: python step3_deduplication.py
Install: pip install thefuzz python-Levenshtein
"""
import duckdb
from thefuzz import fuzz
import re
from pathlib import Path

def resolve_db_path() -> str:
    """Resolve the DuckDB file path from the data directory."""
    data_dir = Path(__file__).resolve().parents[1] / "data"
    preferred = data_dir / "off_v2.duckdb"
    if preferred.exists():
        return str(preferred)

    candidates = sorted(data_dir.glob("*.duckdb"))
    if not candidates:
        raise FileNotFoundError(f"No .duckdb file found in {data_dir}")
    return str(candidates[0])

con = duckdb.connect(resolve_db_path(), read_only=True)

def normalize(text: str) -> str:
    """Normalize product name for comparison."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)  # remove special chars
    text = re.sub(r'\s+', ' ', text)          # normalize spaces
    return text.strip()

def is_duplicate(name1: str, brand1: str, name2: str, brand2: str) -> bool:
    """Check if two products are duplicates using fuzzy matching."""
    # Brand must be similar
    brand_score = fuzz.ratio(normalize(brand1), normalize(brand2))
    if brand_score < 80:
        return False

    # Product name must be very similar
    name_score = fuzz.ratio(normalize(name1), normalize(name2))
    return name_score >= 85

# ----------------------------------------------------------------
# Test on sample of Canadian products
# ----------------------------------------------------------------
print("=" * 60)
print("PRODUCT DEDUPLICATION — SAMPLE TEST")
print("=" * 60)

# Get sample of canadian products with brands
sample = con.execute("""
    SELECT code, product_name, brands
    FROM products
    WHERE primary_country = 'canada'
    AND product_name IS NOT NULL
    AND brands IS NOT NULL
    AND product_name != ''
    AND brands != ''
    ORDER BY brands, product_name
    LIMIT 500
""").fetchdf()

print(f"\nAnalyzing {len(sample)} Canadian products for duplicates...\n")

# Find duplicate groups
groups      = {}
group_id    = 1
assigned    = {}

for i, row1 in sample.iterrows():
    if row1['code'] in assigned:
        continue

    group_key = f"GROUP_{group_id:04d}"
    assigned[row1['code']] = group_key
    group_members = [row1['code']]

    for j, row2 in sample.iterrows():
        if i >= j:
            continue
        if row2['code'] in assigned:
            continue

        if is_duplicate(
            row1['product_name'], row1['brands'],
            row2['product_name'], row2['brands']
        ):
            assigned[row2['code']] = group_key
            group_members.append(row2['code'])

    if len(group_members) > 1:
        groups[group_key] = {
            "members":      group_members,
            "product_name": row1['product_name'],
            "brand":        row1['brands']
        }

    group_id += 1

# Results
total_dupes     = sum(len(g['members']) - 1 for g in groups.values())
multi_groups    = {k: v for k, v in groups.items() if len(v['members']) > 1}

print(f"Results on {len(sample)} sample products:")
print(f"  Duplicate groups found: {len(multi_groups)}")
print(f"  Redundant records:      {total_dupes}")
print(f"  Reduction:              {total_dupes/len(sample)*100:.1f}%")

print(f"\nSample duplicate groups:")
for gid, group in list(multi_groups.items())[:5]:
    print(f"\n  {gid}: '{group['product_name']}' by '{group['brand']}'")
    print(f"  Barcodes: {group['members']}")

    # Show actual data for these products
    codes        = group['members']
    placeholders = ','.join(['?' for _ in codes])
    products     = con.execute(f"""
        SELECT code, product_name, brands, sodium_100g, proteins_100g
        FROM products WHERE code IN ({placeholders})
    """, codes).fetchdf()
    print(f"  Details:")
    for _, p in products.iterrows():
        print(f"    {p['code']} | {p['product_name'][:30]} | sodium: {p['sodium_100g']} | protein: {p['proteins_100g']}")

# Extrapolate to full DB
full_total = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
estimated_dupes = int(total_dupes / len(sample) * full_total)

print(f"\n{'='*60}")
print(f"EXTRAPOLATION TO FULL DATABASE:")
print(f"  Full DB products:      {full_total:,}")
print(f"  Estimated duplicates:  {estimated_dupes:,}")
print(f"  After dedup:           {full_total - estimated_dupes:,} unique products")
print(f"\n→ product_group_id column links duplicates without deleting data")
print(f"→ Search returns 1 result per group instead of {estimated_dupes:,} duplicates")

con.close()