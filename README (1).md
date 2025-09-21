
# HVDC Orchestrator (SKU=Case No. based, Hitachi/Siemens 3-file linkage)

- Bridges three existing scripts with a central `SKU_MASTER` hub.
- Requires local environment to have `STOCK.py`, `hvdc_excel_reporter_final_sqm_rev.py`, and `hvdc wh invoice.py` importable/executable.
- Output: `out/SKU_MASTER.parquet` + `out/sku_master.duckdb`.
