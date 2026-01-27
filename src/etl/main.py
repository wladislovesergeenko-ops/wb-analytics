"""Main ETL entry point with new refactored architecture"""

import math
import time
from datetime import date, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from supabase import create_client

from src.config.settings import get_settings
from src.logging_config.logger import setup_logger, configure_logging
from src.connectors.wb import WBConnector
from src.etl.transformers import WBTransformer
from src.core.exceptions import ETLException, WBConnectorError, OzonConnectorError


logger = setup_logger(__name__)


def sanitize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sanitize records for JSON serialization.
    Converts NaN, inf, -inf, numpy types to JSON-compatible values.
    """
    def sanitize_value(val):
        if val is None:
            return None
        # Handle numpy types first
        if isinstance(val, (np.integer, np.floating)):
            val = val.item()
        # Handle float NaN/inf
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return None
        # Handle pandas NA
        if pd.isna(val):
            return None
        return val

    return [
        {k: sanitize_value(v) for k, v in record.items()}
        for record in records
    ]


def run_adverts_settings_pipeline(supabase, connector: WBConnector, settings) -> None:
    """Run adverts settings refresh pipeline"""
    try:
        logger.info("Starting WB Adverts Settings pipeline")
        
        raw_data = connector.fetch_adverts()
        df = WBTransformer.transform_adverts(raw_data, settings.get_adverts_statuses())
        
        if df.empty:
            logger.warning("No adverts data to upsert")
            return
        
        # Delete old records and insert new ones
        try:
            supabase.table(settings.ADVERTS_TABLE).delete().in_(
                "status", 
                settings.get_adverts_statuses()
            ).execute()
            logger.info(f"Deleted old adverts records for statuses {settings.get_adverts_statuses()}")
        except Exception as e:
            logger.warning(f"Could not delete old adverts: {e}")
        
        # Deduplicate by (advert_id, nmid) - keep last occurrence
        df = df.drop_duplicates(subset=["advert_id", "nmid"], keep="last")

        # Batch upsert
        records = sanitize_records(df.to_dict(orient="records"))
        batch_size = settings.BATCH_SIZE
        for i in range(0, len(records), batch_size):
            supabase.table(settings.ADVERTS_TABLE).upsert(
                records[i : i + batch_size],
                on_conflict="advert_id,nmid"
            ).execute()

        logger.info(f"✅ Adverts settings pipeline completed: {len(records)} records upserted")
    
    except Exception as e:
        logger.error(f"Adverts settings pipeline failed: {e}", exc_info=True)
        raise


def run_sales_funnel_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run sales funnel pipeline for date range"""
    try:
        logger.info(f"Starting WB Sales Funnel pipeline: {date_from} -> {date_to}")
        
        current_date = date_from
        total_records = 0
        
        while current_date <= date_to:
            try:
                logger.info(f"Processing sales funnel for {current_date}")
                
                raw_data = connector.fetch_sales_funnel(current_date.isoformat(), current_date.isoformat())
                df = WBTransformer.transform_sales_funnel(raw_data)
                
                if not df.empty:
                    records = sanitize_records(df.to_dict(orient="records"))
                    
                    # Batch upsert
                    batch_size = settings.BATCH_SIZE
                    for i in range(0, len(records), batch_size):
                        supabase.table(settings.SALES_FUNNEL_TABLE).upsert(
                            records[i : i + batch_size],
                            on_conflict="nmid,periodstart,periodend"
                        ).execute()
                    
                    total_records += len(records)
                    logger.info(f"Upserted {len(records)} sales funnel records for {current_date}")
                else:
                    logger.info(f"No sales funnel data for {current_date}")
                
            except Exception as e:
                logger.error(f"Failed to process {current_date}: {e}", exc_info=True)
            
            # Rate limiting
            if current_date < date_to:
                time.sleep(settings.SLEEP_SECONDS)
            
            current_date += timedelta(days=1)
        
        logger.info(f"✅ Sales funnel pipeline completed: {total_records} total records upserted")
    
    except Exception as e:
        logger.error(f"Sales funnel pipeline failed: {e}", exc_info=True)
        raise


def run_fullstats_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run adverts fullstats pipeline"""
    try:
        logger.info(f"Starting WB Fullstats pipeline: {date_from} -> {date_to}")
        
        # Get active advert IDs from Supabase
        resp = supabase.table(settings.ADVERTS_TABLE).select("advert_id").in_(
            "status",
            settings.get_fullstats_statuses()
        ).limit(10000).execute()
        
        advert_ids = sorted({int(r["advert_id"]) for r in (resp.data or []) if r.get("advert_id")})
        
        if not advert_ids:
            logger.info("No active advert IDs found, skipping fullstats")
            return
        
        logger.info(f"Found {len(advert_ids)} active adverts for fullstats")
        
        # Fetch fullstats data
        raw_data = connector.fetch_fullstats_chunked(
            advert_ids=advert_ids,
            begin_date=date_from.isoformat(),
            end_date=date_to.isoformat(),
            chunk_size=settings.FULLSTATS_CHUNK_SIZE,
            sleep_seconds=settings.FULLSTATS_SLEEP_SECONDS,
        )
        
        # Transform
        df = WBTransformer.transform_fullstats_days(raw_data)
        
        if df.empty:
            logger.warning("No fullstats data to upsert")
            return
        
        # Batch upsert
        records = sanitize_records(df.to_dict(orient="records"))
        batch_size = settings.BATCH_SIZE
        for i in range(0, len(records), batch_size):
            supabase.table(settings.FULLSTATS_TABLE).upsert(
                records[i : i + batch_size],
                on_conflict="advert_id,date"
            ).execute()
        
        logger.info(f"✅ Fullstats pipeline completed: {len(records)} records upserted")
    
    except Exception as e:
        logger.error(f"Fullstats pipeline failed: {e}", exc_info=True)
        raise


def run_spp_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run SPP snapshot pipeline"""
    try:
        logger.info(f"Starting SPP snapshot pipeline: {date_from} -> {date_to}")

        current_date = date_from
        total_records = 0

        while current_date <= date_to:
            try:
                logger.info(f"Processing SPP snapshot for {current_date}")

                raw_data = connector.fetch_orders(current_date.isoformat(), flag=1)
                df = WBTransformer.transform_spp_snapshot(raw_data, current_date.isoformat(), only_not_canceled=True)

                if not df.empty:
                    records = sanitize_records(df.to_dict(orient="records"))

                    # Batch upsert
                    batch_size = settings.BATCH_SIZE
                    for i in range(0, len(records), batch_size):
                        supabase.table(settings.SPP_TABLE).upsert(
                            records[i : i + batch_size],
                            on_conflict="date,nmid"
                        ).execute()

                    total_records += len(records)
                    logger.info(f"Upserted {len(records)} SPP records for {current_date}")
                else:
                    logger.info(f"No SPP data for {current_date}")

            except Exception as e:
                logger.error(f"Failed to process SPP for {current_date}: {e}", exc_info=True)

            # Rate limiting
            if current_date < date_to:
                time.sleep(settings.SLEEP_SECONDS)

            current_date += timedelta(days=1)

        logger.info(f"✅ SPP pipeline completed: {total_records} total records upserted")

    except Exception as e:
        logger.error(f"SPP pipeline failed: {e}", exc_info=True)
        raise


def run_search_report_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run search report pipeline (product positions in search)"""
    try:
        logger.info(f"Starting Search Report pipeline: {date_from} -> {date_to}")

        current_date = date_from
        total_records = 0

        while current_date <= date_to:
            try:
                date_str = current_date.isoformat()
                logger.info(f"Processing search report for {date_str}")

                # Fetch all groups with pagination
                groups = connector.fetch_search_report_all(
                    start=date_str,
                    end=date_str,
                    sleep_seconds=settings.SLEEP_SECONDS,
                )

                if groups:
                    df = WBTransformer.transform_search_report_groups(groups, date_str, date_str)

                    if not df.empty:
                        records = sanitize_records(df.to_dict(orient="records"))

                        # Batch upsert
                        batch_size = settings.BATCH_SIZE
                        for i in range(0, len(records), batch_size):
                            supabase.table(settings.SEARCH_REPORT_TABLE).upsert(
                                records[i : i + batch_size],
                                on_conflict="nm_id,period_start,period_end"
                            ).execute()

                        total_records += len(records)
                        logger.info(f"Upserted {len(records)} search report records for {date_str}")
                    else:
                        logger.info(f"No search report data for {date_str}")
                else:
                    logger.info(f"No groups in search report for {date_str}")

            except Exception as e:
                logger.error(f"Failed to process search report for {current_date}: {e}", exc_info=True)

            # Rate limiting
            if current_date < date_to:
                time.sleep(settings.SLEEP_SECONDS)

            current_date += timedelta(days=1)

        logger.info(f"✅ Search Report pipeline completed: {total_records} total records upserted")

    except Exception as e:
        logger.error(f"Search Report pipeline failed: {e}", exc_info=True)
        raise


def run_search_texts_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run search texts pipeline (search queries per product)"""
    try:
        logger.info(f"Starting Search Texts pipeline: {date_from} -> {date_to}")

        current_date = date_from
        total_records = 0

        while current_date <= date_to:
            try:
                date_str = current_date.isoformat()
                logger.info(f"Processing search texts for {date_str}")

                # Get nm_ids from search_report for this date
                resp = supabase.table(settings.SEARCH_REPORT_TABLE).select("nm_id").eq(
                    "period_start", date_str
                ).execute()

                nm_ids = list({r["nm_id"] for r in (resp.data or []) if r.get("nm_id")})

                if not nm_ids:
                    logger.info(f"No nm_ids in search_report for {date_str}, skipping")
                    current_date += timedelta(days=1)
                    continue

                logger.info(f"Found {len(nm_ids)} products for {date_str}")

                # Fetch search texts with chunking
                items = connector.fetch_product_search_texts_chunked(
                    nm_ids=nm_ids,
                    start=date_str,
                    end=date_str,
                    sleep_seconds=settings.SLEEP_SECONDS,
                )

                if items:
                    df = WBTransformer.transform_product_search_texts(items, date_str, date_str)

                    if not df.empty:
                        records = sanitize_records(df.to_dict(orient="records"))

                        # Batch upsert
                        batch_size = settings.BATCH_SIZE
                        for i in range(0, len(records), batch_size):
                            supabase.table(settings.SEARCH_TEXTS_TABLE).upsert(
                                records[i : i + batch_size],
                                on_conflict="nm_id,text,period_start,period_end"
                            ).execute()

                        total_records += len(records)
                        logger.info(f"Upserted {len(records)} search texts records for {date_str}")
                    else:
                        logger.info(f"No search texts data for {date_str}")
                else:
                    logger.info(f"No search texts for {date_str}")

            except Exception as e:
                logger.error(f"Failed to process search texts for {current_date}: {e}", exc_info=True)

            # Rate limiting
            if current_date < date_to:
                time.sleep(settings.SLEEP_SECONDS)

            current_date += timedelta(days=1)

        logger.info(f"✅ Search Texts pipeline completed: {total_records} total records upserted")

    except Exception as e:
        logger.error(f"Search Texts pipeline failed: {e}", exc_info=True)
        raise


def run_normquery_stats_pipeline(supabase, connector: WBConnector, settings, date_from: date, date_to: date) -> None:
    """Run normquery stats pipeline (search cluster statistics for ad campaigns)"""
    try:
        logger.info(f"Starting Normquery Stats pipeline: {date_from} -> {date_to}")

        # Get active advert_id + nmid pairs from Supabase (status 9 or 11)
        resp = supabase.table(settings.ADVERTS_TABLE).select("advert_id,nmid").in_(
            "status",
            settings.get_fullstats_statuses()
        ).limit(10000).execute()

        # Build items list for API: [{advert_id, nm_id}, ...]
        items = []
        seen = set()
        for r in (resp.data or []):
            advert_id = r.get("advert_id")
            nmid = r.get("nmid")
            if advert_id and nmid:
                key = (int(advert_id), int(nmid))
                if key not in seen:
                    seen.add(key)
                    items.append({"advert_id": int(advert_id), "nm_id": int(nmid)})

        if not items:
            logger.info("No active advert+nm pairs found, skipping normquery stats")
            return

        logger.info(f"Found {len(items)} advert+nm pairs for normquery stats")

        # Fetch normquery stats with chunking (max 100 items per request)
        all_stats = connector.fetch_normquery_stats_chunked(
            items=items,
            date_start=date_from.isoformat(),
            date_end=date_to.isoformat(),
            chunk_size=100,
            sleep_seconds=settings.NORMQUERY_SLEEP_SECONDS,
        )

        # Transform
        df = WBTransformer.transform_normquery_stats(
            all_stats,
            date_from.isoformat(),
            date_to.isoformat()
        )

        if df.empty:
            logger.warning("No normquery stats data to upsert")
            return

        # Batch upsert
        records = sanitize_records(df.to_dict(orient="records"))
        batch_size = settings.BATCH_SIZE
        for i in range(0, len(records), batch_size):
            supabase.table(settings.NORMQUERY_STATS_TABLE).upsert(
                records[i : i + batch_size],
                on_conflict="advert_id,nm_id,date_from,date_to,norm_query"
            ).execute()

        logger.info(f"✅ Normquery Stats pipeline completed: {len(records)} records upserted")

    except Exception as e:
        logger.error(f"Normquery Stats pipeline failed: {e}", exc_info=True)
        raise


def run_ozon_analytics_pipeline(supabase, settings, date_from: date, date_to: date) -> None:
    """Run Ozon Analytics pipeline (sales data)"""
    try:
        from src.connectors.ozon import OzonConnector
        from src.etl.ozon_transformer import OzonTransformer

        logger.info(f"Starting Ozon Analytics pipeline: {date_from} -> {date_to}")

        # Initialize connector
        connector = OzonConnector(
            api_key=settings.OZON_API_KEY,
            client_id=settings.OZON_CLIENT_ID
        )

        if not connector.validate_connection():
            raise OzonConnectorError("Failed to validate Ozon API connection")
        logger.info("✅ Ozon API connection validated")

        # Metrics to fetch
        metrics = [
            "revenue",
            "ordered_units",
            "hits_view_search",
            "hits_view_pdp",
            "hits_view",
            "hits_tocart_search",
            "hits_tocart_pdp",
            "hits_tocart",
            "session_view_search",
            "session_view_pdp",
            "session_view",
            "delivered_units",
            "position_category",
        ]

        # Fetch data with pagination
        all_records = []
        offset = 0
        limit = 1000

        while True:
            raw_data = connector.fetch_analytics_data(
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                metrics=metrics,
                dimensions=["day", "sku"],
                limit=limit,
                offset=offset
            )

            records = OzonTransformer.transform_analytics_data(raw_data, metrics)

            if not records:
                break

            all_records.extend(records)
            logger.info(f"Fetched {len(records)} records (offset={offset})")

            if len(records) < limit:
                break

            offset += limit
            time.sleep(1)  # Rate limiting

        if not all_records:
            logger.warning("No Ozon analytics data to upsert")
            return

        # Filter valid records
        valid_records = [r for r in all_records if OzonTransformer.validate_record(r)]
        logger.info(f"Valid records: {len(valid_records)} / {len(all_records)}")

        # Batch upsert
        batch_size = settings.BATCH_SIZE
        for i in range(0, len(valid_records), batch_size):
            supabase.table(settings.OZON_ANALYTICS_TABLE).upsert(
                valid_records[i : i + batch_size],
                on_conflict="date,sku"
            ).execute()

        logger.info(f"✅ Ozon Analytics pipeline completed: {len(valid_records)} records upserted")

    except Exception as e:
        logger.error(f"Ozon Analytics pipeline failed: {e}", exc_info=True)
        raise


def run_ozon_performance_pipeline(supabase, settings, date_from: date, date_to: date) -> None:
    """Run Ozon Performance pipeline (advertising stats)"""
    try:
        import zipfile
        import io
        from src.connectors.ozon_performance import OzonPerformanceConnector
        from src.etl.ozon_performance_transformer import OzonPerformanceTransformer

        logger.info(f"Starting Ozon Performance pipeline: {date_from} -> {date_to}")

        # Initialize connector
        connector = OzonPerformanceConnector(
            client_id=settings.OZON_PERF_CLIENT_ID,
            client_secret=settings.OZON_PERF_CLIENT_SECRET
        )

        if not connector.validate_connection():
            raise OzonConnectorError("Failed to validate Ozon Performance API connection")
        logger.info("✅ Ozon Performance API connection validated")

        # Fetch all campaigns
        campaigns_data = connector.fetch_campaigns()
        all_campaigns = campaigns_data.get("list", [])

        if not all_campaigns:
            logger.warning("No Ozon campaigns found, skipping performance pipeline")
            return

        # Filter only active campaigns
        campaigns = [c for c in all_campaigns if c.get("state") == "CAMPAIGN_STATE_RUNNING"]
        logger.info(f"Found {len(campaigns)} active campaigns (of {len(all_campaigns)} total)")

        if not campaigns:
            logger.warning("No active Ozon campaigns, skipping")
            return

        # Get campaign IDs
        campaign_ids = [str(c["id"]) for c in campaigns]

        # Process in batches of 10 (API limit)
        batch_size_api = 10
        total_records = 0

        for batch_start in range(0, len(campaign_ids), batch_size_api):
            batch_ids = campaign_ids[batch_start:batch_start + batch_size_api]
            batch_num = batch_start // batch_size_api + 1
            total_batches = (len(campaign_ids) + batch_size_api - 1) // batch_size_api

            logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_ids)} campaigns")

            try:
                # Fetch stats for batch
                report_data = connector.fetch_campaign_product_stats(
                    campaign_ids=batch_ids,
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    max_wait_seconds=300,
                    poll_interval=10
                )

                # Check if ZIP or CSV
                is_zip = report_data[:2] == b'PK'
                all_records = []

                if is_zip:
                    # Extract and parse each CSV from ZIP
                    with zipfile.ZipFile(io.BytesIO(report_data)) as zip_file:
                        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                        for csv_filename in csv_files:
                            csv_data = zip_file.read(csv_filename)
                            records = OzonPerformanceTransformer.parse_csv_report(csv_data)
                            all_records.extend(records)
                else:
                    # Single CSV
                    all_records = OzonPerformanceTransformer.parse_csv_report(report_data)

                if not all_records:
                    logger.info(f"No data in batch {batch_num}")
                    continue

                # Filter valid records
                valid_records = [r for r in all_records if OzonPerformanceTransformer.validate_record(r)]

                # Batch upsert to Supabase
                batch_size_db = settings.BATCH_SIZE
                for i in range(0, len(valid_records), batch_size_db):
                    supabase.table(settings.OZON_PERFORMANCE_TABLE).upsert(
                        valid_records[i : i + batch_size_db],
                        on_conflict="campaign_id,date,sku"
                    ).execute()

                total_records += len(valid_records)
                logger.info(f"Batch {batch_num}: uploaded {len(valid_records)} records")

            except Exception as e:
                logger.error(f"Failed batch {batch_num}: {e}")
                continue

        logger.info(f"✅ Ozon Performance pipeline completed: {total_records} records upserted")

    except Exception as e:
        logger.error(f"Ozon Performance pipeline failed: {e}", exc_info=True)
        raise


def run_ozon_sku_promo_pipeline(supabase, settings, date_from: date, date_to: date):
    """
    Run Ozon SKU Promo (Оплата за заказ) ETL pipeline

    Fetches orders report from Performance API and aggregates by SKU+date.
    """
    from src.connectors.ozon_performance import OzonPerformanceConnector
    from src.etl.ozon_performance_transformer import OzonSkuPromoTransformer

    try:
        logger.info(f"Starting Ozon SKU Promo pipeline: {date_from} -> {date_to}")

        # Initialize connector
        connector = OzonPerformanceConnector(
            client_id=settings.OZON_PERF_CLIENT_ID,
            client_secret=settings.OZON_PERF_CLIENT_SECRET
        )

        # Fetch SKU Promo orders report
        report_data = connector.fetch_sku_promo_orders(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            max_wait_seconds=300,
            poll_interval=10
        )

        if not report_data:
            logger.warning("No SKU Promo data returned")
            return

        # Parse orders
        orders = OzonSkuPromoTransformer.parse_orders_report(report_data)
        logger.info(f"Parsed {len(orders)} SKU Promo orders")

        if not orders:
            logger.warning("No valid orders parsed")
            return

        # Aggregate by SKU + date
        aggregated = OzonSkuPromoTransformer.aggregate_by_sku_date(orders)
        logger.info(f"Aggregated into {len(aggregated)} SKU+date records")

        if not aggregated:
            return

        # Filter valid records
        valid_records = [r for r in aggregated if OzonSkuPromoTransformer.validate_record(r)]

        # Sanitize and upsert
        records = sanitize_records(valid_records)
        batch_size = settings.BATCH_SIZE

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            supabase.table(settings.OZON_SKU_PROMO_TABLE).upsert(
                batch,
                on_conflict="date,sku"
            ).execute()

        logger.info(f"✅ Ozon SKU Promo pipeline completed: {len(records)} records upserted")

    except Exception as e:
        logger.error(f"Ozon SKU Promo pipeline failed: {e}", exc_info=True)
        raise


def main():
    """Main ETL orchestration function"""
    try:
        logger.info("=" * 80)
        logger.info("Starting ETL Application")
        logger.info("=" * 80)
        
        # Load settings from .env
        settings = get_settings()
        
        # Configure logging with settings
        configure_logging(
            log_level=settings.LOG_LEVEL,
            log_dir=settings.LOG_DIR,
            log_to_file=settings.LOG_TO_FILE,
        )
        
        # Initialize Supabase client
        sb_key = settings.get_supabase_key()
        if not sb_key:
            raise RuntimeError("No Supabase key available (SERVICE_ROLE or ANON)")
        
        supabase = create_client(settings.SUPABASE_URL, sb_key)
        logger.info("✅ Supabase client initialized")
        
        # Initialize WB connector
        connector = WBConnector(settings.WB_KEY)
        
        # Validate connection
        if not connector.validate_connection():
            raise WBConnectorError("Failed to validate WB API connection")
        logger.info("✅ WB API connection validated")
        
        # Calculate date ranges
        date_to = date.today() - timedelta(days=1)  # Yesterday
        date_from = date_to - timedelta(days=settings.OVERLAP_DAYS)
        
        logger.info(f"Processing period: {date_from} -> {date_to}")
        
        # Run pipelines based on flags
        if settings.RUN_ADVERTS_SETTINGS:
            run_adverts_settings_pipeline(supabase, connector, settings)
        
        if settings.RUN_SALES_FUNNEL:
            run_sales_funnel_pipeline(supabase, connector, settings, date_from, date_to)
        
        if settings.RUN_ADVERTS_FULLSTATS:
            run_fullstats_pipeline(supabase, connector, settings, date_to, date_to)
        
        if settings.RUN_SPP:
            spp_start = date_to - timedelta(days=settings.SPP_OVERLAP_DAYS - 1)
            run_spp_pipeline(supabase, connector, settings, spp_start, date_to)

        # Search Report pipeline (must run before Search Texts)
        if settings.RUN_SEARCH_REPORT:
            run_search_report_pipeline(supabase, connector, settings, date_to, date_to)

        # Search Texts pipeline (depends on Search Report data)
        if settings.RUN_SEARCH_TEXTS:
            run_search_texts_pipeline(supabase, connector, settings, date_to, date_to)

        # Normquery Stats pipeline (search cluster statistics) - single day only
        if settings.RUN_NORMQUERY_STATS:
            run_normquery_stats_pipeline(supabase, connector, settings, date_to, date_to)

        # Ozon Analytics pipeline
        if settings.RUN_OZON_ANALYTICS:
            if settings.OZON_API_KEY and settings.OZON_CLIENT_ID:
                run_ozon_analytics_pipeline(supabase, settings, date_from, date_to)
            else:
                logger.warning("Ozon Analytics enabled but OZON_API_KEY/OZON_CLIENT_ID not set")

        # Ozon Performance pipeline
        if settings.RUN_OZON_PERFORMANCE:
            if settings.OZON_PERF_CLIENT_ID and settings.OZON_PERF_CLIENT_SECRET:
                run_ozon_performance_pipeline(supabase, settings, date_from, date_to)
            else:
                logger.warning("Ozon Performance enabled but OZON_PERF_CLIENT_ID/OZON_PERF_CLIENT_SECRET not set")

        # Ozon SKU Promo pipeline (Оплата за заказ)
        if settings.RUN_OZON_SKU_PROMO:
            if settings.OZON_PERF_CLIENT_ID and settings.OZON_PERF_CLIENT_SECRET:
                run_ozon_sku_promo_pipeline(supabase, settings, date_from, date_to)
            else:
                logger.warning("Ozon SKU Promo enabled but OZON_PERF_CLIENT_ID/OZON_PERF_CLIENT_SECRET not set")

        logger.info("=" * 80)
        logger.info("✅ All ETL pipelines completed successfully!")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"Fatal error in main ETL: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
