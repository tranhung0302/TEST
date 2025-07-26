# ASSIGNMENT

## Setup

```bash
conda create -n test python=3.10
```
```bash
conda activate test
```
```bash
pip install -r requirements.txt
```

## Running Job
```bash
python main.py \
    --invoices data/invoices.csv \
    --credit_notes data/credit_notes.csv \
    --payments data/payments.csv \
    --as_at_date 2025-07-07 \
    --output data/output/fact_table.csv
```
For CLI instructions
```bash
python main.py --help
```