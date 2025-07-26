import pandas as pd
import argparse

BUCKETS = {
    "day_30": (0, 30),
    "day_60": (31, 60),
    "day_90": (61, 90),
    "day_120": (91, 120),
    "day_150": (121, 150),
    "day_180": (151, 180),
    "day_180_and_above": (181, float("inf")),
}

def assign_bucket(date, as_at_date):
    age = (as_at_date - date).days
    for bucket, (start, end) in BUCKETS.items():
        if start <= age <= end:
            return bucket
    return None

def load_data(invoices_path, credit_notes_path, payments_path):
    invoices = pd.read_csv(invoices_path, parse_dates=["invoice_date"])
    credit_notes = pd.read_csv(credit_notes_path, parse_dates=["credit_note_date"])
    payments = pd.read_csv(payments_path, parse_dates=["payment_date"])
    return invoices, credit_notes, payments


def transform_documents(df, doc_type, date_col):
    df = df.rename(columns={
        "id": "document_id",
        date_col: "document_date"
    })
    df["document_type"] = doc_type
    return df

def generate_fact_table(invoices, credit_notes, payments, as_date_at):
    # Combine invoices and credit notes
    invoices = transform_documents(invoices, "invoice", "invoice_date")
    credit_notes = transform_documents(credit_notes, "credit_note", "credit_note_date")
    all_docs = pd.concat([invoices, credit_notes], ignore_index=True)

    # Calculate outstanding
    payments_grouped = payments.groupby("document_id")["amount_paid"].sum().reset_index()
    all_docs = all_docs.merge(payments_grouped, on="document_id", how="left").fillna(0)
    all_docs["outstanding_amount"] = all_docs["total_amount"] - all_docs["amount_paid"]

    # Keep only documents with outstanding amount
    all_docs = all_docs[all_docs["outstanding_amount"] > 0]

    # Assign bucket
    all_docs["bucket"] = all_docs.apply(lambda row: assign_bucket(row["document_date"], as_date_at), axis=1)

    # Aggregate buckets columns
    buckets = all_docs.pivot_table(
        index=['document_id'],
        columns='bucket',
        values='outstanding_amount',
        fill_value=0.0
    ).reset_index()

    all_docs = pd.merge(all_docs, buckets, on="document_id", how="inner")

    all_docs["as_at_date"] = as_date_at

    return all_docs[[
        "centre_id",
        "class_id",
        "document_id",
        "document_date",
        "day_30",
        "day_60",
        "day_90",
        "day_120",
        "day_150",
        "day_180",
        "day_180_and_above",
        "document_type",
        "as_at_date"
    ]]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ageing fact table.")
    parser.add_argument("--invoices", type=str, default="data/invoices.csv", help="Path to invoices.csv")
    parser.add_argument("--credit_notes", type=str, default="data/credit_notes.csv", help="Path to credit_notes.csv")
    parser.add_argument("--payments", type=str, default="data/payments.csv", help="Path to payments.csv")
    parser.add_argument("--as_at_date", type=str, default="2025-07-07", help="As at date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="data/output/ageing.csv", help="Output CSV path")
    args = parser.parse_args()

    INVOICES_PATH = args.invoices
    CREDIT_NOTES_PATH = args.credit_notes
    PAYMENTS_PATH = args.payments
    AS_AT_DATE = pd.Timestamp(args.as_at_date)
    OUTPUT_PATH = args.output

    invoices, credit_notes, payments = load_data(INVOICES_PATH, CREDIT_NOTES_PATH, PAYMENTS_PATH)
    fact_table = generate_fact_table(invoices, credit_notes, payments, AS_AT_DATE)
    fact_table.to_csv(OUTPUT_PATH, index=False)
