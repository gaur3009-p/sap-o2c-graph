"""
Phase 1: SAP Order-to-Cash — Data Ingestion & Graph Modeler
============================================================
Reads raw JSONL files from the SAP O2C dataset, normalises and cleans
the data, then structures it into typed Node and Edge records ready
for database insertion.

Graph schema produced
─────────────────────
Nodes:
  Customer            ← business_partners + customer_*_assignments
  Address             ← business_partner_addresses
  SalesOrder          ← sales_order_headers
  SalesOrderItem      ← sales_order_items  (also carries schedule line data)
  Product             ← products + product_descriptions
  OutboundDelivery    ← outbound_delivery_headers + outbound_delivery_items
  BillingDocument     ← billing_document_headers + billing_document_items
  JournalEntry        ← journal_entry_items_accounts_receivable
  Payment             ← payments_accounts_receivable

Edges  (source → target):
  Customer      -[:PLACED]->           SalesOrder
  Customer      -[:HAS_ADDRESS]->      Address
  SalesOrder    -[:CONTAINS]->         SalesOrderItem
  SalesOrderItem-[:REFERENCES]->       Product
  SalesOrder    -[:HAS_DELIVERY]->     OutboundDelivery
  OutboundDelivery-[:BILLED_IN]->      BillingDocument
  BillingDocument-[:RECORDED_IN]->     JournalEntry
  JournalEntry  -[:SETTLED_BY]->       Payment

Output
──────
  output/nodes/  — one CSV per node type
  output/edges/  — one CSV per relationship type

Usage
─────
  python phase1_ingest.py --data-dir ./sap-o2c-data --output-dir ./output
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_jsonl_folder(folder: Path) -> pd.DataFrame:
    """
    Concatenate all *.jsonl part-files in a folder into one DataFrame.
    Skips blank lines silently.
    """
    records = []
    for path in sorted(folder.glob("*.jsonl")):
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        log.warning("Skipping malformed line in %s: %s", path.name, exc)
    if not records:
        raise FileNotFoundError(f"No JSONL records found in {folder}")
    df = pd.DataFrame(records)
    log.info("  Loaded %-45s  %d rows, %d cols", folder.name, len(df), len(df.columns))
    return df


def clean_str(series: pd.Series) -> pd.Series:
    """Strip whitespace; replace empty strings with None."""
    return series.astype(str).str.strip().replace({"": None, "nan": None, "None": None})


def parse_date(series: pd.Series) -> pd.Series:
    """
    Parse ISO-8601 date strings to date-only strings (YYYY-MM-DD).
    Handles null, epoch-null (9999-12-31), and malformed values gracefully.
    """
    def _parse(val):
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        if not s or s.lower() in ("none", "nat", "null"):
            return None
        if s.startswith("9999"):          # SAP open-ended sentinel
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return series.apply(_parse)


def coerce_numeric(series: pd.Series) -> pd.Series:
    """Convert to float; non-parseable values become None."""
    return pd.to_numeric(series, errors="coerce")


def deduplicate(df: pd.DataFrame, key_col: str, label: str) -> pd.DataFrame:
    """Keep the first occurrence of each key; log how many dupes were dropped."""
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log.info("  Deduplicated %-20s  dropped %d duplicate rows on '%s'", label, dropped, key_col)
    return df


def save(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("  Saved  %s  (%d rows)", path.relative_to(path.parent.parent.parent), len(df))


# ── Node builders ─────────────────────────────────────────────────────────────

def build_customers(raw: dict) -> pd.DataFrame:
    """
    Merge business_partners + customer_company_assignments + customer_sales_area_assignments.
    The business_partners table is the master; the assignment tables enrich with
    payment terms, currency, and credit control metadata.
    """
    log.info("Building Customer nodes …")
    bp = raw["business_partners"].copy()
    cca = raw["customer_company_assignments"].copy()
    csa = raw["customer_sales_area_assignments"].copy()

    # Normalise partner/customer id column name — some tables call it 'customer'
    bp = bp.rename(columns={"businessPartner": "customer_id"})
    cca = cca.rename(columns={"customer": "customer_id"})
    csa = csa.rename(columns={"customer": "customer_id"})

    # Keep one row per customer in the assignment tables (take first sales area)
    cca_lean = (
        cca[["customer_id", "companyCode", "reconciliationAccount"]]
        .drop_duplicates("customer_id")
    )
    csa_lean = (
        csa[["customer_id", "currency", "customerPaymentTerms", "creditControlArea",
              "salesOrganization", "distributionChannel"]]
        .drop_duplicates("customer_id")
    )

    df = (
        bp
        .merge(cca_lean, on="customer_id", how="left")
        .merge(csa_lean, on="customer_id", how="left")
    )

    out = pd.DataFrame({
        "customer_id":          clean_str(df["customer_id"]),
        "full_name":            clean_str(df["businessPartnerFullName"]),
        "bp_category":          clean_str(df["businessPartnerCategory"]),
        "is_blocked":           df["businessPartnerIsBlocked"].fillna(False),
        "is_archived":          df["isMarkedForArchiving"].fillna(False),
        "company_code":         clean_str(df["companyCode"]),
        "currency":             clean_str(df["currency"]),
        "payment_terms":        clean_str(df["customerPaymentTerms"]),
        "sales_organization":   clean_str(df["salesOrganization"]),
        "creation_date":        parse_date(df["creationDate"]),
    })

    out = deduplicate(out, "customer_id", "Customer")
    log.info("  → %d Customer nodes", len(out))
    return out


def build_addresses(raw: dict) -> pd.DataFrame:
    log.info("Building Address nodes …")
    df = raw["business_partner_addresses"].copy()

    out = pd.DataFrame({
        "address_id":       clean_str(df["addressId"]),
        "customer_id":      clean_str(df["businessPartner"]),   # FK kept for edge building
        "street":           clean_str(df["streetName"]),
        "city":             clean_str(df["cityName"]),
        "postal_code":      clean_str(df["postalCode"]),
        "region":           clean_str(df["region"]),
        "country":          clean_str(df["country"]),
        "timezone":         clean_str(df["addressTimeZone"]),
        "valid_from":       parse_date(df["validityStartDate"]),
        "valid_to":         parse_date(df["validityEndDate"]),   # None = open-ended (9999)
    })

    out = deduplicate(out, "address_id", "Address")
    log.info("  → %d Address nodes", len(out))
    return out


def build_sales_orders(raw: dict) -> pd.DataFrame:
    """
    Sales order header + schedule line confirmation data rolled up per order.
    """
    log.info("Building SalesOrder nodes …")
    hdr = raw["sales_order_headers"].copy()
    sched = raw["sales_order_schedule_lines"].copy()

    if not sched.empty:
        sched["confirmedDeliveryDate"] = pd.to_datetime(
            sched["confirmedDeliveryDate"], errors="coerce"
        )

        sched_clean = sched.dropna(subset=["confirmedDeliveryDate"])

        if not sched_clean.empty:
            confirmed = (
                sched_clean.groupby("salesOrder")["confirmedDeliveryDate"]
                .min()
                .reset_index()
                .rename(columns={"confirmedDeliveryDate": "confirmed_delivery_date"})
            )

            confirmed["confirmed_delivery_date"] = confirmed[
                "confirmed_delivery_date"
            ].dt.strftime("%Y-%m-%d")

            hdr = hdr.merge(confirmed, on="salesOrder", how="left")
        else:
            hdr["confirmed_delivery_date"] = None
    else:
        hdr["confirmed_delivery_date"] = None

    out = pd.DataFrame({
        "sales_order_id":           clean_str(hdr["salesOrder"]),
        "order_type":               clean_str(hdr["salesOrderType"]),
        "sold_to_party":            clean_str(hdr["soldToParty"]),
        "sales_organization":       clean_str(hdr["salesOrganization"]),
        "distribution_channel":     clean_str(hdr["distributionChannel"]),
        "division":                 clean_str(hdr["organizationDivision"]),
        "total_net_amount":         coerce_numeric(hdr["totalNetAmount"]),
        "currency":                 clean_str(hdr["transactionCurrency"]),
        "creation_date":            parse_date(hdr["creationDate"]),
        "pricing_date":             parse_date(hdr["pricingDate"]),
        "requested_delivery_date":  parse_date(hdr["requestedDeliveryDate"]),
        "confirmed_delivery_date":  hdr["confirmed_delivery_date"],
        "delivery_status":          clean_str(hdr["overallDeliveryStatus"]),
        "billing_status":           clean_str(hdr["overallOrdReltdBillgStatus"]),
        "delivery_block":           clean_str(hdr["deliveryBlockReason"]),
        "billing_block":            clean_str(hdr["headerBillingBlockReason"]),
        "payment_terms":            clean_str(hdr["customerPaymentTerms"]),
        "incoterms":                clean_str(hdr["incotermsClassification"]),
    })

    out = deduplicate(out, "sales_order_id", "SalesOrder")
    log.info("  → %d SalesOrder nodes", len(out))
    return out

def build_sales_order_items(raw: dict) -> pd.DataFrame:
    log.info("Building SalesOrderItem nodes …")
    df = raw["sales_order_items"].copy()

    out = pd.DataFrame({
        # Composite key: order_id + item_number
        "item_id":          clean_str(df["salesOrder"]) + "_" + clean_str(df["salesOrderItem"]),
        "sales_order_id":   clean_str(df["salesOrder"]),    # FK → SalesOrder
        "item_number":      clean_str(df["salesOrderItem"]),
        "material":         clean_str(df["material"]),       # FK → Product
        "item_category":    clean_str(df["salesOrderItemCategory"]),
        "requested_qty":    coerce_numeric(df["requestedQuantity"]),
        "qty_unit":         clean_str(df["requestedQuantityUnit"]),
        "net_amount":       coerce_numeric(df["netAmount"]),
        "currency":         clean_str(df["transactionCurrency"]),
        "material_group":   clean_str(df["materialGroup"]),
        "plant":            clean_str(df["productionPlant"]),
        "storage_location": clean_str(df["storageLocation"]),
        "rejection_reason": clean_str(df["salesDocumentRjcnReason"]),
        "billing_block":    clean_str(df["itemBillingBlockReason"]),
    })

    out = deduplicate(out, "item_id", "SalesOrderItem")
    log.info("  → %d SalesOrderItem nodes", len(out))
    return out


def build_products(raw: dict) -> pd.DataFrame:
    log.info("Building Product nodes …")
    prod = raw["products"].copy()
    desc = raw["product_descriptions"].copy()

    # Keep only English descriptions
    desc_en = (
        desc[desc["language"] == "EN"][["product", "productDescription"]]
        .drop_duplicates("product")
    )

    df = prod.merge(desc_en, on="product", how="left")

    out = pd.DataFrame({
        "product_id":       clean_str(df["product"]),
        "product_type":     clean_str(df["productType"]),
        "description":      clean_str(df["productDescription"]),
        "old_product_id":   clean_str(df["productOldId"]),
        "product_group":    clean_str(df["productGroup"]),
        "base_unit":        clean_str(df["baseUnit"]),
        "gross_weight":     coerce_numeric(df["grossWeight"]),
        "net_weight":       coerce_numeric(df["netWeight"]),
        "weight_unit":      clean_str(df["weightUnit"]),
        "division":         clean_str(df["division"]),
        "industry_sector":  clean_str(df["industrySector"]),
        "is_deleted":       df["isMarkedForDeletion"].fillna(False),
        "creation_date":    parse_date(df["creationDate"]),
    })

    out = deduplicate(out, "product_id", "Product")
    log.info("  → %d Product nodes", len(out))
    return out


def build_deliveries(raw: dict) -> pd.DataFrame:
    """
    Merge outbound_delivery_headers (one row per delivery doc) with
    outbound_delivery_items to create a flattened delivery node.
    Each delivery document becomes one node; item-level data is stored
    as JSON-serialised arrays for reference.
    """
    log.info("Building OutboundDelivery nodes …")
    hdr = raw["outbound_delivery_headers"].copy()
    itm = raw["outbound_delivery_items"].copy()

    # Roll up item-level data: collect referenced sales orders, items, quantities
    itm_agg = itm.groupby("deliveryDocument").agg(
        total_delivery_qty=("actualDeliveryQuantity", lambda x: sum(float(v) for v in x if v)),
        reference_sales_orders=("referenceSdDocument", lambda x: list(x.unique())),
        ref_sales_order_items=("referenceSdDocumentItem", lambda x: list(x.unique())),
        plants=("plant", lambda x: list(x.unique())),
    ).reset_index()

    df = hdr.merge(itm_agg, on="deliveryDocument", how="left")

    out = pd.DataFrame({
        "delivery_id":              clean_str(df["deliveryDocument"]),
        "shipping_point":           clean_str(df["shippingPoint"]),
        "creation_date":            parse_date(df["creationDate"]),
        "actual_goods_mvmt_date":   parse_date(df["actualGoodsMovementDate"]),
        "picking_status":           clean_str(df["overallPickingStatus"]),
        "goods_movement_status":    clean_str(df["overallGoodsMovementStatus"]),
        "delivery_block":           clean_str(df["deliveryBlockReason"]),
        "billing_block":            clean_str(df["headerBillingBlockReason"]),
        "incompletion_status":      clean_str(df["hdrGeneralIncompletionStatus"]),
        "total_delivery_qty":       df["total_delivery_qty"],
        # Serialised arrays — used in Phase 2 to build SHIPS edges
        "reference_sales_orders":   df["reference_sales_orders"].apply(
                                        lambda x: json.dumps(x) if isinstance(x, list) else None),
    })

    out = deduplicate(out, "delivery_id", "OutboundDelivery")
    log.info("  → %d OutboundDelivery nodes", len(out))
    return out


def build_billing_documents(raw: dict) -> pd.DataFrame:
    """
    Merge billing_document_headers + billing_document_items.
    Cancelled billing documents (billingDocumentIsCancelled = True) are
    flagged but kept — they are important for broken-flow detection in Phase 5.
    """
    log.info("Building BillingDocument nodes …")
    hdr = raw["billing_document_headers"].copy()
    itm = raw["billing_document_items"].copy()
    canc = raw["billing_document_cancellations"].copy()

    # Mark cancelled documents
    cancelled_ids = set(clean_str(canc["billingDocument"]).dropna())
    hdr["is_cancelled"] = clean_str(hdr["billingDocument"]).isin(cancelled_ids)

    # Roll up items: referenced delivery documents, products
    itm_agg = itm.groupby("billingDocument").agg(
        reference_deliveries=("referenceSdDocument", lambda x: list(x.unique())),
        materials=("material", lambda x: list(x.unique())),
        total_items=("billingDocumentItem", "count"),
    ).reset_index()

    df = hdr.merge(itm_agg, on="billingDocument", how="left")

    out = pd.DataFrame({
        "billing_doc_id":       clean_str(df["billingDocument"]),
        "billing_doc_type":     clean_str(df["billingDocumentType"]),
        "sold_to_party":        clean_str(df["soldToParty"]),        # FK → Customer
        "accounting_document":  clean_str(df["accountingDocument"]), # FK → JournalEntry
        "company_code":         clean_str(df["companyCode"]),
        "fiscal_year":          clean_str(df["fiscalYear"]),
        "total_net_amount":     coerce_numeric(df["totalNetAmount"]),
        "currency":             clean_str(df["transactionCurrency"]),
        "billing_date":         parse_date(df["billingDocumentDate"]),
        "creation_date":        parse_date(df["creationDate"]),
        "is_cancelled":         df["is_cancelled"].fillna(False),
        "total_items":          df["total_items"].fillna(0).astype(int),
        # Serialised arrays — used to build BILLED_IN edges
        "reference_deliveries": df["reference_deliveries"].apply(
                                    lambda x: json.dumps(x) if isinstance(x, list) else None),
        "materials":            df["materials"].apply(
                                    lambda x: json.dumps(x) if isinstance(x, list) else None),
    })

    out = deduplicate(out, "billing_doc_id", "BillingDocument")
    log.info("  → %d BillingDocument nodes", len(out))
    return out


def build_journal_entries(raw: dict) -> pd.DataFrame:
    log.info("Building JournalEntry nodes …")
    df = raw["journal_entry_items_accounts_receivable"].copy()

    out = pd.DataFrame({
        # Composite key: accounting_document + item
        "journal_entry_id":     (clean_str(df["accountingDocument"]) + "_"
                                  + clean_str(df["accountingDocumentItem"])),
        "accounting_document":  clean_str(df["accountingDocument"]),  # FK links to BillingDoc
        "reference_document":   clean_str(df["referenceDocument"]),   # FK → BillingDocument
        "company_code":         clean_str(df["companyCode"]),
        "fiscal_year":          clean_str(df["fiscalYear"]),
        "gl_account":           clean_str(df["glAccount"]),
        "customer":             clean_str(df["customer"]),             # FK → Customer
        "profit_center":        clean_str(df["profitCenter"]),
        "amount":               coerce_numeric(df["amountInTransactionCurrency"]),
        "currency":             clean_str(df["transactionCurrency"]),
        "posting_date":         parse_date(df["postingDate"]),
        "document_date":        parse_date(df["documentDate"]),
        "doc_type":             clean_str(df["accountingDocumentType"]),
        "clearing_date":        parse_date(df["clearingDate"]),
        "clearing_document":    clean_str(df["clearingAccountingDocument"]), # FK → Payment
    })

    out = deduplicate(out, "journal_entry_id", "JournalEntry")
    log.info("  → %d JournalEntry nodes", len(out))
    return out


def build_payments(raw: dict) -> pd.DataFrame:
    log.info("Building Payment nodes …")
    df = raw["payments_accounts_receivable"].copy()

    out = pd.DataFrame({
        "payment_id":           (clean_str(df["accountingDocument"]) + "_"
                                  + clean_str(df["accountingDocumentItem"])),
        "accounting_document":  clean_str(df["accountingDocument"]),
        "clearing_document":    clean_str(df["clearingAccountingDocument"]), # actual payment doc
        "company_code":         clean_str(df["companyCode"]),
        "fiscal_year":          clean_str(df["fiscalYear"]),
        "customer":             clean_str(df["customer"]),             # FK → Customer
        "gl_account":           clean_str(df["glAccount"]),
        "amount":               coerce_numeric(df["amountInTransactionCurrency"]),
        "currency":             clean_str(df["transactionCurrency"]),
        "posting_date":         parse_date(df["postingDate"]),
        "clearing_date":        parse_date(df["clearingDate"]),
        "document_date":        parse_date(df["documentDate"]),
    })

    out = deduplicate(out, "payment_id", "Payment")
    log.info("  → %d Payment nodes", len(out))
    return out


# ── Edge builders ─────────────────────────────────────────────────────────────

def build_edges(nodes: dict) -> dict:
    """
    Build all edge DataFrames from the already-cleaned node tables.
    Every edge has: source_id, target_id, and optional properties.

    Edge map (SAP O2C flow):
      Customer -[:PLACED]->           SalesOrder       via soldToParty
      Customer -[:HAS_ADDRESS]->      Address          via customer_id
      SalesOrder -[:CONTAINS]->       SalesOrderItem   via sales_order_id
      SalesOrderItem -[:REFERENCES]-> Product          via material
      SalesOrder -[:HAS_DELIVERY]->   OutboundDelivery via delivery.reference_sales_orders
      OutboundDelivery -[:BILLED_IN]->BillingDocument  via billing.reference_deliveries
      BillingDocument -[:RECORDED_IN]->JournalEntry    via accountingDocument
      JournalEntry -[:SETTLED_BY]->   Payment          via clearing_document
    """
    log.info("Building edges …")
    edges = {}

    # ── Customer PLACED SalesOrder ─────────────────────────────────────────
    so = nodes["sales_orders"][["sales_order_id", "sold_to_party"]].dropna()
    edges["customer_placed_order"] = pd.DataFrame({
        "source_id":    so["sold_to_party"],
        "target_id":    so["sales_order_id"],
        "relationship": "PLACED",
    }).dropna()
    log.info("  PLACED edges:              %d", len(edges["customer_placed_order"]))

    # ── Customer HAS_ADDRESS Address ──────────────────────────────────────
    addr = nodes["addresses"][["address_id", "customer_id"]].dropna()
    edges["customer_has_address"] = pd.DataFrame({
        "source_id":    addr["customer_id"],
        "target_id":    addr["address_id"],
        "relationship": "HAS_ADDRESS",
    }).dropna()
    log.info("  HAS_ADDRESS edges:         %d", len(edges["customer_has_address"]))

    # ── SalesOrder CONTAINS SalesOrderItem ────────────────────────────────
    soi = nodes["sales_order_items"][["item_id", "sales_order_id"]].dropna()
    edges["order_contains_item"] = pd.DataFrame({
        "source_id":    soi["sales_order_id"],
        "target_id":    soi["item_id"],
        "relationship": "CONTAINS",
    }).dropna()
    log.info("  CONTAINS edges:            %d", len(edges["order_contains_item"]))

    # ── SalesOrderItem REFERENCES Product ────────────────────────────────
    soi_prod = nodes["sales_order_items"][["item_id", "material"]].dropna()
    # Only keep items where the material exists in our product catalogue
    valid_products = set(nodes["products"]["product_id"].dropna())
    soi_prod = soi_prod[soi_prod["material"].isin(valid_products)]
    edges["item_references_product"] = pd.DataFrame({
        "source_id":    soi_prod["item_id"],
        "target_id":    soi_prod["material"],
        "relationship": "REFERENCES",
    }).dropna()
    log.info("  REFERENCES edges:          %d", len(edges["item_references_product"]))

    # ── SalesOrder HAS_DELIVERY OutboundDelivery ──────────────────────────
    # Deliveries reference their source sales orders in a JSON array column
    deliv = nodes["deliveries"][["delivery_id", "reference_sales_orders"]].dropna()
    order_delivery_rows = []
    for _, row in deliv.iterrows():
        refs = json.loads(row["reference_sales_orders"]) if row["reference_sales_orders"] else []
        for so_id in refs:
            order_delivery_rows.append({
                "source_id":    so_id,
                "target_id":    row["delivery_id"],
                "relationship": "HAS_DELIVERY",
            })
    edges["order_has_delivery"] = pd.DataFrame(order_delivery_rows).dropna()
    log.info("  HAS_DELIVERY edges:        %d", len(edges["order_has_delivery"]))

    # ── OutboundDelivery BILLED_IN BillingDocument ─────────────────────────
    # Billing documents reference their source delivery documents in a JSON array
    bill = nodes["billing_docs"][["billing_doc_id", "reference_deliveries"]].dropna()
    delivery_billing_rows = []
    for _, row in bill.iterrows():
        refs = json.loads(row["reference_deliveries"]) if row["reference_deliveries"] else []
        for deliv_id in refs:
            delivery_billing_rows.append({
                "source_id":    deliv_id,
                "target_id":    row["billing_doc_id"],
                "relationship": "BILLED_IN",
            })
    edges["delivery_billed_in"] = pd.DataFrame(delivery_billing_rows).dropna()
    log.info("  BILLED_IN edges:           %d", len(edges["delivery_billed_in"]))

    # ── BillingDocument RECORDED_IN JournalEntry ──────────────────────────
    # journal.reference_document == billing.billing_doc_id
    je = nodes["journal_entries"][["journal_entry_id", "reference_document"]].dropna()
    edges["billing_recorded_in_journal"] = pd.DataFrame({
        "source_id":    je["reference_document"],   # billing_doc_id
        "target_id":    je["journal_entry_id"],
        "relationship": "RECORDED_IN",
    }).dropna()
    log.info("  RECORDED_IN edges:         %d", len(edges["billing_recorded_in_journal"]))

    # ── JournalEntry SETTLED_BY Payment ───────────────────────────────────
    # journal.clearing_document matches payment.clearing_document (the clearing FI doc)
    je_full = nodes["journal_entries"][["journal_entry_id", "clearing_document"]].dropna()
    pay = nodes["payments"][["payment_id", "clearing_document"]].dropna()
    # Join on clearing_document
    je_pay = je_full.merge(pay, on="clearing_document", how="inner")
    edges["journal_settled_by_payment"] = pd.DataFrame({
        "source_id":    je_pay["journal_entry_id"],
        "target_id":    je_pay["payment_id"],
        "relationship": "SETTLED_BY",
    }).dropna()
    log.info("  SETTLED_BY edges:          %d", len(edges["journal_settled_by_payment"]))

    return edges


# ── Validation ────────────────────────────────────────────────────────────────

def validate(nodes: dict, edges: dict) -> None:
    """
    Run basic referential integrity checks and print a summary report.
    Warnings are logged but do not halt the pipeline — they indicate
    data gaps in the source system (e.g. partial exports, cancelled docs).
    """
    log.info("Running validation …")

    checks = [
        ("PLACED",        "customer_placed_order",         "source_id", "customers",      "customer_id"),
        ("PLACED",        "customer_placed_order",         "target_id", "sales_orders",   "sales_order_id"),
        ("HAS_ADDRESS",   "customer_has_address",          "source_id", "customers",      "customer_id"),
        ("CONTAINS",      "order_contains_item",           "source_id", "sales_orders",   "sales_order_id"),
        ("REFERENCES",    "item_references_product",       "target_id", "products",       "product_id"),
        ("HAS_DELIVERY",  "order_has_delivery",            "source_id", "sales_orders",   "sales_order_id"),
        ("BILLED_IN",     "delivery_billed_in",            "source_id", "deliveries",     "delivery_id"),
        ("RECORDED_IN",   "billing_recorded_in_journal",   "source_id", "billing_docs",   "billing_doc_id"),
    ]

    all_ok = True
    for rel, edge_key, col, node_key, id_col in checks:
        edge_ids   = set(edges[edge_key][col].dropna())
        node_ids   = set(nodes[node_key][id_col].dropna())
        dangling   = edge_ids - node_ids
        if dangling:
            log.warning(
                "  [%s] %d dangling %s references (e.g. %s)",
                rel, len(dangling), col, list(dangling)[:3]
            )
            all_ok = False

    if all_ok:
        log.info("  All referential integrity checks passed ✓")

    # Flow completeness report
    log.info("Flow completeness summary:")
    so_ids        = set(nodes["sales_orders"]["sales_order_id"].dropna())
    delivered_ids = set(edges["order_has_delivery"]["source_id"].dropna())
    billed_del    = set(edges["delivery_billed_in"]["source_id"].dropna())
    has_deliv     = set(edges["order_has_delivery"]["target_id"].dropna())
    billed_ids    = set(edges["delivery_billed_in"]["target_id"].dropna())
    journaled_ids = set(edges["billing_recorded_in_journal"]["source_id"].dropna())
    settled_ids   = set(edges["journal_settled_by_payment"]["source_id"].dropna())

    log.info("  Sales orders total:                          %4d", len(so_ids))
    log.info("  Sales orders with ≥1 delivery:               %4d  (%.0f%%)",
             len(delivered_ids), 100 * len(delivered_ids) / max(len(so_ids), 1))
    log.info("  Deliveries billed:                           %4d / %d",
             len(billed_del), len(has_deliv))
    log.info("  Billing docs with journal entry:             %4d / %d",
             len(journaled_ids), len(billed_ids))
    log.info("  Journal entries settled (payment received):  %4d / %d",
             len(settled_ids), len(journaled_ids))
    undelivered = so_ids - delivered_ids
    if undelivered:
        log.info("  ⚠ Orders never delivered:                  %4d  (broken flow candidates)",
                 len(undelivered))
    unbilled = has_deliv - billed_del
    if unbilled:
        log.info("  ⚠ Deliveries never billed:                 %4d  (broken flow candidates)",
                 len(unbilled))
    unsettled = set(journaled_ids) - set(settled_ids)
    if unsettled:
        log.info("  ⚠ Journal entries without payment:         %4d  (open receivables)",
                 len(unsettled))


# ── Main ──────────────────────────────────────────────────────────────────────

def main(data_dir: Path, output_dir: Path) -> None:
    log.info("=" * 60)
    log.info("Phase 1 — SAP O2C Graph Ingestion Pipeline")
    log.info("Data dir :  %s", data_dir)
    log.info("Output dir: %s", output_dir)
    log.info("=" * 60)

    # ── 1. Load raw JSONL files ───────────────────────────────────────────
    log.info("Step 1/4 — Loading raw JSONL data …")
    raw = {
        "sales_order_headers":                    load_jsonl_folder(data_dir / "sales_order_headers"),
        "sales_order_items":                      load_jsonl_folder(data_dir / "sales_order_items"),
        "sales_order_schedule_lines":             load_jsonl_folder(data_dir / "sales_order_schedule_lines"),
        "billing_document_headers":               load_jsonl_folder(data_dir / "billing_document_headers"),
        "billing_document_items":                 load_jsonl_folder(data_dir / "billing_document_items"),
        "billing_document_cancellations":         load_jsonl_folder(data_dir / "billing_document_cancellations"),
        "outbound_delivery_headers":              load_jsonl_folder(data_dir / "outbound_delivery_headers"),
        "outbound_delivery_items":                load_jsonl_folder(data_dir / "outbound_delivery_items"),
        "payments_accounts_receivable":           load_jsonl_folder(data_dir / "payments_accounts_receivable"),
        "journal_entry_items_accounts_receivable":load_jsonl_folder(data_dir / "journal_entry_items_accounts_receivable"),
        "business_partners":                      load_jsonl_folder(data_dir / "business_partners"),
        "business_partner_addresses":             load_jsonl_folder(data_dir / "business_partner_addresses"),
        "customer_company_assignments":           load_jsonl_folder(data_dir / "customer_company_assignments"),
        "customer_sales_area_assignments":        load_jsonl_folder(data_dir / "customer_sales_area_assignments"),
        "products":                               load_jsonl_folder(data_dir / "products"),
        "product_descriptions":                   load_jsonl_folder(data_dir / "product_descriptions"),
    }

    # ── 2. Build node tables ──────────────────────────────────────────────
    log.info("Step 2/4 — Building node tables …")
    nodes = {
        "customers":        build_customers(raw),
        "addresses":        build_addresses(raw),
        "sales_orders":     build_sales_orders(raw),
        "sales_order_items":build_sales_order_items(raw),
        "products":         build_products(raw),
        "deliveries":       build_deliveries(raw),
        "billing_docs":     build_billing_documents(raw),
        "journal_entries":  build_journal_entries(raw),
        "payments":         build_payments(raw),
    }

    # ── 3. Build edge tables ──────────────────────────────────────────────
    log.info("Step 3/4 — Building edge tables …")
    edges = build_edges(nodes)

    # ── 4. Validate & save ────────────────────────────────────────────────
    log.info("Step 4/4 — Validating and saving …")
    validate(nodes, edges)

    for name, df in nodes.items():
        save(df, output_dir / "nodes" / f"{name}.csv")

    for name, df in edges.items():
        save(df, output_dir / "edges" / f"{name}.csv")

    log.info("=" * 60)
    log.info("Phase 1 complete.  Outputs written to: %s", output_dir)
    log.info("  Nodes: %d files", len(nodes))
    log.info("  Edges: %d files", len(edges))
    log.info("=" * 60)
    log.info("Next step: Review the validation output above, then proceed to Phase 2.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP O2C — Phase 1 Graph Ingestion")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("sap-o2c-data"),
        help="Root folder containing the JSONL entity sub-folders",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Destination folder for node/edge CSVs",
    )
    args = parser.parse_args()

    if not args.data_dir.exists():
        log.error("Data directory not found: %s", args.data_dir)
        sys.exit(1)

    main(args.data_dir, args.output_dir)
