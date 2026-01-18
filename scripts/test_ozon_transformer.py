"""Test Ozon transformer"""

import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.ozon_transformer import OzonTransformer


def test_transformer():
    """Test transformation of Ozon analytics data"""
    
    # Load test response
    test_file = Path("logs/ozon_test_response.json")
    
    if not test_file.exists():
        print("❌ Test file not found. Run test_ozon_simple.py first.")
        return
    
    with open(test_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    print("="*80)
    print("Testing OzonTransformer")
    print("="*80)
    
    # Define metrics order
    metrics_order = ["revenue", "ordered_units"]
    
    # Transform
    transformer = OzonTransformer()
    transformed = transformer.transform_analytics_data(raw_data, metrics_order)
    
    print(f"\n✅ Transformed {len(transformed)} records")
    
    # Show first 3 records
    print("\nFirst 3 transformed records:")
    for i, record in enumerate(transformed[:3], 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, ensure_ascii=False))
        
        # Validate
        is_valid = transformer.validate_record(record)
        print(f"Valid: {is_valid}")
    
    print("\n" + "="*80)
    print("✅ Transformer test completed!")


if __name__ == "__main__":
    test_transformer()