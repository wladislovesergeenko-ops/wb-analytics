import os
from dotenv import load_dotenv
from supabase import create_client

from src.etl.spp_snapshot import load_spp_snapshot_to_supabase


supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
wb_key = os.getenv("WB_KEY")

load_spp_snapshot_to_supabase(
    wb_key=wb_key,
    supabase=supabase,
    date_from="2026-01-11",
    date_to="2026-01-11",
    sleep_seconds=0.0,
)