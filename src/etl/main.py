"""Main ETL entry point with new refactored architecture"""

import time
from datetime import date, timedelta
from supabase import create_client

from src.config.settings import get_settings
from src.logging_config.logger import setup_logger, configure_logging
from src.connectors.wb import WBConnector
from src.etl.transformers import WBTransformer
from src.core.exceptions import ETLException, WBConnectorError


logger = setup_logger(__name__)


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
        
        # Batch insert
        records = df.to_dict(orient="records")
        batch_size = settings.BATCH_SIZE
        for i in range(0, len(records), batch_size):
            supabase.table(settings.ADVERTS_TABLE).insert(
                records[i : i + batch_size]
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
                    records = df.to_dict(orient="records")
                    
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
        records = df.to_dict(orient="records")
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
                    records = df.to_dict(orient="records")

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
                        records = df.to_dict(orient="records")

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
                        records = df.to_dict(orient="records")

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

        # Future Ozon pipelines
        if settings.OZON_ENABLED and settings.RUN_OZON_PRODUCTS:
            logger.warning("Ozon pipelines coming soon (not implemented yet)")
        
        logger.info("=" * 80)
        logger.info("✅ All ETL pipelines completed successfully!")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"Fatal error in main ETL: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
