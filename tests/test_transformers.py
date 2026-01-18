"""Unit tests for data transformers"""

import pytest
import pandas as pd
from datetime import date

from src.etl.transformers import WBTransformer
from src.core.exceptions import TransformationError


class TestSalesFunnelTransformer:
    """Tests for sales funnel transformation"""
    
    def test_transform_sales_funnel_empty(self):
        """Test transformation with empty data"""
        raw_data = {"data": {"products": []}}
        result = WBTransformer.transform_sales_funnel(raw_data)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_transform_sales_funnel_basic(self):
        """Test basic sales funnel transformation"""
        raw_data = {
            "data": {
                "products": [
                    {
                        "product__nmId": 123,
                        "product__title": "Test Product",
                        "product__vendorCode": "SKU001",
                        "product__brandName": "Brand",
                        "product__subjectId": 1,
                        "product__subjectName": "Category",
                        "product__feedbackRating": 4.5,
                        "product__stocks__wb": 100,
                        "statistic__selected__openCount": 1000,
                        "statistic__selected__cartCount": 50,
                        "statistic__selected__orderCount": 10,
                        "statistic__selected__orderSum": 1000,
                        "statistic__selected__buyoutCount": 8,
                        "statistic__selected__buyoutSum": 800,
                        "statistic__selected__cancelCount": 2,
                        "statistic__selected__cancelSum": 200,
                        "statistic__selected__avgPrice": 100,
                        "statistic__selected__localizationPercent": 80,
                        "statistic__selected__period__start": "2026-01-01",
                        "statistic__selected__period__end": "2026-01-01",
                        "statistic__selected__timeToReady__days": 1,
                        "statistic__selected__timeToReady__hours": 2,
                    }
                ]
            }
        }
        
        result = WBTransformer.transform_sales_funnel(raw_data)
        
        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]["nmid"] == 123
        assert result.iloc[0]["title"] == "Test Product"
        assert result.iloc[0]["opencount"] == 1000


class TestAdvertsTransformer:
    """Tests for adverts transformation"""
    
    def test_transform_adverts_empty(self):
        """Test transformation with empty adverts"""
        raw_data = {"adverts": []}
        result = WBTransformer.transform_adverts(raw_data, [9, 11])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_transform_adverts_filter_by_status(self):
        """Test filtering adverts by status"""
        raw_data = {
            "adverts": [
                {
                    "id": 100,
                    "status": 9,
                    "bid_type": "cpc",
                    "settings": {
                        "name": "Campaign 1",
                        "payment_type": "CPA",
                        "placements": {"search": True, "recommendations": False},
                    },
                    "timestamps": {},
                    "nm_settings": [
                        {
                            "nm_id": 123,
                            "bids_kopecks": {"search": 50, "recommendations": 30},
                            "subject": {"id": 1, "name": "Category"},
                        }
                    ],
                },
                {
                    "id": 101,
                    "status": 7,  # This should be filtered out
                    "bid_type": "cpc",
                    "settings": {
                        "name": "Campaign 2",
                        "payment_type": "CPA",
                        "placements": {"search": False, "recommendations": True},
                    },
                    "timestamps": {},
                    "nm_settings": [
                        {
                            "nm_id": 456,
                            "bids_kopecks": {"search": 40, "recommendations": 20},
                            "subject": {"id": 2, "name": "Category2"},
                        }
                    ],
                }
            ]
        }
        
        result = WBTransformer.transform_adverts(raw_data, [9, 11])
        
        assert len(result) == 1
        assert result.iloc[0]["advert_id"] == 100
        assert result.iloc[0]["nmid"] == 123


class TestFullstatsTransformer:
    """Tests for fullstats transformation"""
    
    def test_transform_fullstats_empty(self):
        """Test transformation with empty data"""
        result = WBTransformer.transform_fullstats_days([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_transform_fullstats_basic(self):
        """Test basic fullstats transformation"""
        raw_data = [
            {
                "advertId": 100,
                "days": [
                    {
                        "date": "2026-01-15",
                        "atbs": 0,
                        "views": 1000,
                        "clicks": 50,
                        "orders": 5,
                        "canceled": 0,
                        "shks": 100,
                        "sum": 50000,
                        "sum_price": 10000,
                        "cpc": 100,
                        "ctr": 5.0,
                        "cr": 10.0,
                        "raw": None,
                        "boosterStats": {"some": "data"},  # Should be ignored
                    }
                ]
            }
        ]
        
        result = WBTransformer.transform_fullstats_days(raw_data)
        
        assert len(result) == 1
        assert result.iloc[0]["advert_id"] == 100
        assert result.iloc[0]["date"] == "2026-01-15"
        assert result.iloc[0]["views"] == 1000
        assert "boosterstats" not in result.columns


class TestSPPTransformer:
    """Tests for SPP snapshot transformation"""
    
    def test_transform_spp_snapshot_empty(self):
        """Test transformation with empty data"""
        result = WBTransformer.transform_spp_snapshot([], "2026-01-15")
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_transform_spp_snapshot_basic(self):
        """Test basic SPP snapshot transformation"""
        raw_data = [
            {"nmId": 123, "spp": 10.5, "finishedPrice": 1000, "isCancel": False},
            {"nmId": 123, "spp": 11.0, "finishedPrice": 1050, "isCancel": False},
            {"nmId": 456, "spp": 9.0, "finishedPrice": 900, "isCancel": False},
        ]
        
        result = WBTransformer.transform_spp_snapshot(raw_data, "2026-01-15")
        
        # Should have 2 rows (one per nmid, taking first value)
        assert len(result) == 2
        assert result.iloc[0]["nmid"] in [123, 456]
        assert result.iloc[0]["date"] == "2026-01-15"
    
    def test_transform_spp_snapshot_filter_canceled(self):
        """Test SPP snapshot filtering canceled orders"""
        raw_data = [
            {"nmId": 123, "spp": 10.5, "finishedPrice": 1000, "isCancel": False},
            {"nmId": 123, "spp": 11.0, "finishedPrice": 1050, "isCancel": True},  # Should be filtered
        ]
        
        result = WBTransformer.transform_spp_snapshot(raw_data, "2026-01-15", only_not_canceled=True)
        
        assert len(result) == 1
        assert result.iloc[0]["spp"] == 10.5  # First non-canceled value
