"""Data transformers for ETL pipelines"""

from typing import Dict, List, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.logging_config.logger import setup_logger
from src.core.exceptions import TransformationError

logger = setup_logger(__name__)


class WBTransformer:
    """Transformer for Wildberries data"""
    
    @staticmethod
    def transform_sales_funnel(raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform sales funnel raw API data to flat structure
        
        Args:
            raw_data: Raw API response with sales funnel data
        
        Returns:
            Transformed dataframe with sales funnel products
        """
        try:
            rows = raw_data.get("data", {}).get("products", [])
            if not rows:
                logger.warning("No products in sales funnel data")
                return pd.DataFrame()
            
            df = pd.json_normalize(rows, sep="__")
            
            # Define columns to keep
            keep = [
                # product
                "product__nmId",
                "product__title",
                "product__vendorCode",
                "product__brandName",
                "product__subjectId",
                "product__subjectName",
                "product__feedbackRating",
                "product__stocks__wb",
                # statistic.selected
                "statistic__selected__openCount",
                "statistic__selected__cartCount",
                "statistic__selected__orderCount",
                "statistic__selected__orderSum",
                "statistic__selected__buyoutCount",
                "statistic__selected__buyoutSum",
                "statistic__selected__cancelCount",
                "statistic__selected__cancelSum",
                "statistic__selected__avgPrice",
                "statistic__selected__localizationPercent",
                # period
                "statistic__selected__period__start",
                "statistic__selected__period__end",
                # timeToReady
                "statistic__selected__timeToReady__days",
                "statistic__selected__timeToReady__hours",
            ]
            
            df_out = df[[c for c in keep if c in df.columns]].copy()
            
            # Rename columns
            rename_map = {
                "product__nmId": "nmid",
                "product__title": "title",
                "product__vendorCode": "vendorcode",
                "product__brandName": "brandname",
                "product__subjectId": "subjectid",
                "product__subjectName": "subjectname",
                "product__feedbackRating": "feedbackrating",
                "product__stocks__wb": "stocks",
                "statistic__selected__openCount": "opencount",
                "statistic__selected__cartCount": "cartcount",
                "statistic__selected__orderCount": "ordercount",
                "statistic__selected__orderSum": "ordersum",
                "statistic__selected__buyoutCount": "buyoutcount",
                "statistic__selected__buyoutSum": "buyoutsum",
                "statistic__selected__cancelCount": "cancelcount",
                "statistic__selected__cancelSum": "cancelsum",
                "statistic__selected__avgPrice": "avgprice",
                "statistic__selected__localizationPercent": "localizationpercent",
                "statistic__selected__period__start": "periodstart",
                "statistic__selected__period__end": "periodend",
                "statistic__selected__timeToReady__days": "timetoready_days",
                "statistic__selected__timeToReady__hours": "timetoready_hours",
            }
            
            df_out = df_out.rename(columns=rename_map)
            
            # Convert NaN to None (Supabase requirement)
            df_out = df_out.where(pd.notnull(df_out), None)
            
            # Lowercase all columns
            df_out.columns = df_out.columns.str.lower()
            
            logger.info(f"Transformed sales funnel data: {len(df_out)} rows")
            return df_out
        
        except Exception as e:
            logger.error(f"Error transforming sales funnel data: {e}", exc_info=True)
            raise TransformationError(f"Sales funnel transformation failed: {e}") from e
    
    @staticmethod
    def transform_adverts(raw_data: Dict[str, Any], statuses: List[int]) -> pd.DataFrame:
        """
        Transform adverts raw data to flat structure with nm_settings
        
        Args:
            raw_data: Raw API response with adverts
            statuses: List of statuses to filter by
        
        Returns:
            Transformed dataframe with adverts
        """
        try:
            adverts = raw_data.get("adverts", []) or []
            if not adverts:
                logger.warning("No adverts in data")
                return pd.DataFrame()
            
            rows: List[Dict[str, Any]] = []
            statuses_set = set(int(s) for s in statuses)
            
            for adv in adverts:
                status = adv.get("status")
                if status not in statuses_set:
                    continue
                
                advert_id = adv.get("id")
                bid_type = adv.get("bid_type")
                
                settings = adv.get("settings") or {}
                campaign_name = settings.get("name")
                payment_type = settings.get("payment_type")
                
                placements = settings.get("placements") or {}
                place_search = bool(placements.get("search"))
                place_recommendations = bool(placements.get("recommendations"))
                
                ts = adv.get("timestamps") or {}
                ts_created = WBTransformer._to_date_str(ts.get("created"))
                ts_started = WBTransformer._to_date_str(ts.get("started"))
                ts_updated = WBTransformer._to_date_str(ts.get("updated"))
                ts_deleted = WBTransformer._to_date_str(ts.get("deleted"))
                
                # Flatten nm_settings
                nm_settings = adv.get("nm_settings") or []
                for s in nm_settings:
                    nmid = s.get("nm_id")
                    
                    bids = (s.get("bids_kopecks") or {})
                    bid_search_kopecks = bids.get("search")
                    bid_recommendations_kopecks = bids.get("recommendations")
                    
                    subj = (s.get("subject") or {})
                    subject_id = subj.get("id")
                    subject_name = subj.get("name")
                    
                    rows.append({
                        "advert_id": advert_id,
                        "nmid": nmid,
                        "status": status,
                        "bid_type": bid_type,
                        "payment_type": payment_type,
                        "campaign_name": campaign_name,
                        "place_search": place_search,
                        "place_recommendations": place_recommendations,
                        "bid_search_kopecks": bid_search_kopecks,
                        "bid_recommendations_kopecks": bid_recommendations_kopecks,
                        "subject_id": subject_id,
                        "subject_name": subject_name,
                        "ts_created": ts_created,
                        "ts_started": ts_started,
                        "ts_updated": ts_updated,
                        "ts_deleted": ts_deleted,
                    })
            
            if not rows:
                logger.warning("No adverts after filtering by statuses")
                return pd.DataFrame()

            df = pd.DataFrame(rows)

            # Replace inf/-inf with None (JSON doesn't support infinity)
            df = df.replace([np.inf, -np.inf], None)

            # Replace NaN with None
            df = df.where(pd.notnull(df), None)

            logger.info(f"Transformed adverts data: {len(df)} rows")
            return df
        
        except Exception as e:
            logger.error(f"Error transforming adverts data: {e}", exc_info=True)
            raise TransformationError(f"Adverts transformation failed: {e}") from e
    
    @staticmethod
    def transform_fullstats_days(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform fullstats raw data to daily records
        
        Args:
            raw_data: List of fullstats records from API
        
        Returns:
            Dataframe with daily fullstats
        """
        try:
            if not raw_data:
                logger.warning("No fullstats data provided")
                return pd.DataFrame()
            
            rows: List[Dict[str, Any]] = []
            
            for adv in raw_data:
                advert_id = adv.get("advertId") or adv.get("advert_id") or adv.get("id")
                if advert_id is None:
                    continue
                
                days = adv.get("days") or []
                for d in days:
                    day_date = d.get("date")
                    day_copy = dict(d)
                    day_copy.pop("boosterStats", None)  # Ignore booster stats
                    
                    row = {"advert_id": int(advert_id), **day_copy}
                    
                    # Normalize date
                    dt_parsed = pd.to_datetime(day_date, errors="coerce")
                    row["date"] = None if pd.isna(dt_parsed) else dt_parsed.strftime("%Y-%m-%d")
                    
                    rows.append(row)
            
            if not rows:
                logger.warning("No daily records in fullstats")
                return pd.DataFrame()
            
            df = pd.json_normalize(rows, sep="__")
            
            # Ensure required columns
            schema_cols = [
                "advert_id", "date", "atbs", "views", "clicks", "orders", "canceled", "shks",
                "sum", "sum_price", "cpc", "ctr", "cr", "raw"
            ]
            
            for c in schema_cols:
                if c not in df.columns:
                    df[c] = None
            
            df = df[schema_cols].copy()
            
            # Type conversions
            df["advert_id"] = pd.to_numeric(df["advert_id"], errors="coerce")
            for c in ["atbs", "views", "clicks", "orders", "canceled", "shks"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            for c in ["sum", "sum_price", "cpc", "ctr", "cr"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            
            # Date normalization
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            
            # NaN to None
            df = df.where(pd.notnull(df), None)
            
            # Lowercase
            df.columns = df.columns.str.lower()
            
            logger.info(f"Transformed fullstats data: {len(df)} daily records")
            return df
        
        except Exception as e:
            logger.error(f"Error transforming fullstats data: {e}", exc_info=True)
            raise TransformationError(f"Fullstats transformation failed: {e}") from e
    
    @staticmethod
    def transform_spp_snapshot(raw_data: List[Dict[str, Any]], date_str: str, only_not_canceled: bool = True) -> pd.DataFrame:
        """
        Transform orders data to SPP snapshot (1 row per nmid per day)
        
        Args:
            raw_data: Raw orders data from API
            date_str: Date in 'YYYY-MM-DD' format
            only_not_canceled: Filter out canceled orders
        
        Returns:
            Dataframe with SPP snapshot
        """
        try:
            if not raw_data:
                logger.warning(f"No orders data for date {date_str}")
                return pd.DataFrame()
            
            df = pd.DataFrame(raw_data)
            
            if only_not_canceled and "isCancel" in df.columns:
                df = df[df["isCancel"] == False]
            
            # Check needed columns
            need_cols = ["nmId", "spp", "finishedPrice"]
            missing = [c for c in need_cols if c not in df.columns]
            if missing:
                raise TransformationError(f"Missing columns in orders response: {missing}")
            
            df = df[need_cols].copy()
            df["date"] = date_str
            
            # Create snapshot: 1 row per nmid (take first spp and price)
            snap = (
                df.dropna(subset=["nmId"])
                  .groupby("nmId", as_index=False)
                  .agg(
                      spp=("spp", "first"),
                      finished_price=("finishedPrice", "first"),
                      date=("date", "first"),
                  )
                  .rename(columns={"nmId": "nmid"})
            )
            
            snap = snap.where(pd.notnull(snap), None)
            
            logger.info(f"Transformed SPP snapshot for {date_str}: {len(snap)} nmids")
            return snap
        
        except Exception as e:
            logger.error(f"Error transforming SPP data: {e}", exc_info=True)
            raise TransformationError(f"SPP transformation failed: {e}") from e
    
    @staticmethod
    def _to_date_str(x) -> str | None:
        """
        Convert WB timestamp to YYYY-MM-DD string

        Args:
            x: Timestamp value

        Returns:
            Date string or None
        """
        if not x:
            return None
        try:
            dt = pd.to_datetime(x, errors="coerce", utc=False)
            if pd.isna(dt):
                return None
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    # ==================== NEW TRANSFORMERS ====================

    @staticmethod
    def transform_tariffs_commission(raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform tariffs commission data.
        Result: 1 row per subject (category).

        Args:
            raw_data: Raw API response with commission data

        Returns:
            Dataframe with commission rates per subject
        """
        try:
            report = raw_data.get("report", [])
            if not report:
                logger.warning("No commission data in response")
                return pd.DataFrame()

            df = pd.DataFrame(report)

            # Rename to snake_case
            rename_map = {
                "subjectID": "subject_id",
                "subjectName": "subject_name",
                "parentID": "parent_id",
                "parentName": "parent_name",
                "kgvpMarketplace": "commission_fbs",
                "paidStorageKgvp": "commission_fbw",
                "kgvpSupplier": "commission_dbs",
                "kgvpSupplierExpress": "commission_edbs",
                "kgvpBooking": "commission_booking",
                "kgvpPickup": "commission_pickup",
            }

            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

            # Add timestamp
            df["updated_at"] = datetime.utcnow().isoformat()

            df = df.where(pd.notnull(df), None)

            logger.info(f"Transformed commission data: {len(df)} subjects")
            return df

        except Exception as e:
            logger.error(f"Error transforming commission data: {e}", exc_info=True)
            raise TransformationError(f"Commission transformation failed: {e}") from e

    @staticmethod
    def transform_search_report_groups(
        groups: List[Dict[str, Any]],
        period_start: str,
        period_end: str,
    ) -> pd.DataFrame:
        """
        Transform search report groups to flat product rows.
        Result: 1 row per product (nmId) per day.

        Args:
            groups: List of groups from search report response
            period_start: Period start date 'YYYY-MM-DD'
            period_end: Period end date 'YYYY-MM-DD'

        Returns:
            Dataframe with product search metrics
        """
        try:
            if not groups:
                logger.warning("No groups in search report data")
                return pd.DataFrame()

            rows: List[Dict[str, Any]] = []

            for group in groups:
                subject_id = group.get("subjectId")
                subject_name = group.get("subjectName")
                brand_name = group.get("brandName")
                tag_id = group.get("tagId")
                tag_name = group.get("tagName")

                items = group.get("items", [])
                for item in items:
                    row = {
                        "period_start": period_start,
                        "period_end": period_end,
                        "subject_id": subject_id,
                        "subject_name": subject_name,
                        "brand_name": brand_name,
                        "tag_id": tag_id,
                        "tag_name": tag_name,
                        "nm_id": item.get("nmId"),
                        "name": item.get("name"),
                        "vendor_code": item.get("vendorCode"),
                        "is_advertised": item.get("isAdvertised"),
                        "is_card_rated": item.get("isCardRated"),
                        "rating": item.get("rating"),
                        "feedback_rating": item.get("feedbackRating"),
                        "price_min": (item.get("price") or {}).get("minPrice"),
                        "price_max": (item.get("price") or {}).get("maxPrice"),
                        # Metrics - current values
                        "avg_position": WBTransformer._get_metric_current(item, "avgPosition"),
                        "avg_position_dynamics": WBTransformer._get_metric_dynamics(item, "avgPosition"),
                        "open_card": WBTransformer._get_metric_current(item, "openCard"),
                        "open_card_dynamics": WBTransformer._get_metric_dynamics(item, "openCard"),
                        "add_to_cart": WBTransformer._get_metric_current(item, "addToCart"),
                        "add_to_cart_dynamics": WBTransformer._get_metric_dynamics(item, "addToCart"),
                        "open_to_cart": WBTransformer._get_metric_current(item, "openToCart"),
                        "open_to_cart_dynamics": WBTransformer._get_metric_dynamics(item, "openToCart"),
                        "orders": WBTransformer._get_metric_current(item, "orders"),
                        "orders_dynamics": WBTransformer._get_metric_dynamics(item, "orders"),
                        "cart_to_order": WBTransformer._get_metric_current(item, "cartToOrder"),
                        "cart_to_order_dynamics": WBTransformer._get_metric_dynamics(item, "cartToOrder"),
                        "visibility": WBTransformer._get_metric_current(item, "visibility"),
                        "visibility_dynamics": WBTransformer._get_metric_dynamics(item, "visibility"),
                    }
                    rows.append(row)

            if not rows:
                logger.warning("No items in search report groups")
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            df = df.where(pd.notnull(df), None)

            logger.info(f"Transformed search report: {len(df)} product rows")
            return df

        except Exception as e:
            logger.error(f"Error transforming search report: {e}", exc_info=True)
            raise TransformationError(f"Search report transformation failed: {e}") from e

    @staticmethod
    def transform_product_search_texts(
        items: List[Dict[str, Any]],
        period_start: str,
        period_end: str,
    ) -> pd.DataFrame:
        """
        Transform product search texts to flat rows.
        Result: 1 row per search query per product.

        Args:
            items: List of items from search texts response
            period_start: Period start date 'YYYY-MM-DD'
            period_end: Period end date 'YYYY-MM-DD'

        Returns:
            Dataframe with search queries per product
        """
        try:
            if not items:
                logger.warning("No items in search texts data")
                return pd.DataFrame()

            rows: List[Dict[str, Any]] = []

            for item in items:
                row = {
                    "period_start": period_start,
                    "period_end": period_end,
                    "text": item.get("text"),
                    "nm_id": item.get("nmId"),
                    "subject_name": item.get("subjectName"),
                    "brand_name": item.get("brandName"),
                    "vendor_code": item.get("vendorCode"),
                    "name": item.get("name"),
                    "is_card_rated": item.get("isCardRated"),
                    "rating": item.get("rating"),
                    "feedback_rating": item.get("feedbackRating"),
                    "price_min": (item.get("price") or {}).get("minPrice"),
                    "price_max": (item.get("price") or {}).get("maxPrice"),
                    # Frequency
                    "frequency": WBTransformer._get_metric_current(item, "frequency"),
                    "frequency_dynamics": WBTransformer._get_metric_dynamics(item, "frequency"),
                    "week_frequency": item.get("weekFrequency"),
                    # Position metrics
                    "median_position": WBTransformer._get_metric_current(item, "medianPosition"),
                    "median_position_dynamics": WBTransformer._get_metric_dynamics(item, "medianPosition"),
                    "avg_position": WBTransformer._get_metric_current(item, "avgPosition"),
                    "avg_position_dynamics": WBTransformer._get_metric_dynamics(item, "avgPosition"),
                    # Conversion metrics
                    "open_card": WBTransformer._get_metric_current(item, "openCard"),
                    "open_card_dynamics": WBTransformer._get_metric_dynamics(item, "openCard"),
                    "open_card_percentile": WBTransformer._get_metric_percentile(item, "openCard"),
                    "add_to_cart": WBTransformer._get_metric_current(item, "addToCart"),
                    "add_to_cart_dynamics": WBTransformer._get_metric_dynamics(item, "addToCart"),
                    "add_to_cart_percentile": WBTransformer._get_metric_percentile(item, "addToCart"),
                    "open_to_cart": WBTransformer._get_metric_current(item, "openToCart"),
                    "open_to_cart_dynamics": WBTransformer._get_metric_dynamics(item, "openToCart"),
                    "open_to_cart_percentile": WBTransformer._get_metric_percentile(item, "openToCart"),
                    "orders": WBTransformer._get_metric_current(item, "orders"),
                    "orders_dynamics": WBTransformer._get_metric_dynamics(item, "orders"),
                    "orders_percentile": WBTransformer._get_metric_percentile(item, "orders"),
                    "cart_to_order": WBTransformer._get_metric_current(item, "cartToOrder"),
                    "cart_to_order_dynamics": WBTransformer._get_metric_dynamics(item, "cartToOrder"),
                    "cart_to_order_percentile": WBTransformer._get_metric_percentile(item, "cartToOrder"),
                    "visibility": WBTransformer._get_metric_current(item, "visibility"),
                    "visibility_dynamics": WBTransformer._get_metric_dynamics(item, "visibility"),
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            df = df.where(pd.notnull(df), None)

            logger.info(f"Transformed search texts: {len(df)} query rows")
            return df

        except Exception as e:
            logger.error(f"Error transforming search texts: {e}", exc_info=True)
            raise TransformationError(f"Search texts transformation failed: {e}") from e

    @staticmethod
    def _get_metric_current(item: Dict, metric_name: str):
        """Extract 'current' value from metric object"""
        metric = item.get(metric_name)
        if isinstance(metric, dict):
            return metric.get("current")
        return metric

    @staticmethod
    def _get_metric_dynamics(item: Dict, metric_name: str):
        """Extract 'dynamics' value from metric object"""
        metric = item.get(metric_name)
        if isinstance(metric, dict):
            return metric.get("dynamics")
        return None

    @staticmethod
    def _get_metric_percentile(item: Dict, metric_name: str):
        """Extract 'percentile' value from metric object"""
        metric = item.get(metric_name)
        if isinstance(metric, dict):
            return metric.get("percentile")
        return None
