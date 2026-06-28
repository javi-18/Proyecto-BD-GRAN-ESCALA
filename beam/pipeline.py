import argparse
import csv
import os
import uuid
from datetime import datetime, timezone

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq



VALID_STATUS = {"CREATED", "PENDING", "COMPLETED", "REFUNDED"}


class ParseSales(beam.DoFn):
    REJECTED = "rejected"

    def process(self, line):
        row = next(csv.DictReader([line], fieldnames=[
            "transaction_id", "store_id", "sku", "amount", "currency"
        ]))

        transaction_id = row["transaction_id"].strip()
        sku = row["sku"].strip()

        if not transaction_id or not sku:
            yield beam.pvalue.TaggedOutput(self.REJECTED, {
                "source": "sales_data",
                "reason": "campo_critico_nulo",
                "raw_record": line
            })
            return

        try:
            amount = float(row["amount"])
        except ValueError:
            yield beam.pvalue.TaggedOutput(self.REJECTED, {
                "source": "sales_data",
                "reason": "amount_no_numerico",
                "raw_record": line
            })
            return

        if amount <= 0:
            yield beam.pvalue.TaggedOutput(self.REJECTED, {
                "source": "sales_data",
                "reason": "amount_menor_o_igual_a_cero",
                "raw_record": line
            })
            return

        yield transaction_id, {
            "transaction_id": transaction_id,
            "store_id": row["store_id"].strip(),
            "sku": sku,
            "amount": amount,
            "currency": row["currency"].strip()
        }


class ParseStatusLogs(beam.DoFn):
    REJECTED = "rejected"

    def process(self, line):
        row = next(csv.DictReader([line], fieldnames=[
            "transaction_id", "status_name", "status_date"
        ]))

        transaction_id = row["transaction_id"].strip()
        status_name = row["status_name"].strip()

        if not transaction_id or not status_name:
            yield beam.pvalue.TaggedOutput(self.REJECTED, {
                "source": "status_logs",
                "reason": "campo_critico_nulo",
                "raw_record": line
            })
            return

        if status_name not in VALID_STATUS:
            yield beam.pvalue.TaggedOutput(self.REJECTED, {
                "source": "status_logs",
                "reason": "estado_invalido",
                "raw_record": line
            })
            return

        yield transaction_id, {
            "status": status_name,
            "date": row["status_date"].strip()
        }


class BuildNestedRecord(beam.DoFn):
    def __init__(self, batch_id):
        self.batch_id = batch_id

    def process(self, element):
        transaction_id, grouped = element

        sales = grouped["sales"]
        logs = grouped["logs"]

        if not sales:
            return

        sale = sales[0]

        status_history = []
        for log in logs:
            status_history.append({
                "status": log["status"],
                "date": datetime.fromisoformat(log["date"].replace("Z", "+00:00"))
            })

        yield {
            "id": transaction_id,
            "store": sale["store_id"],
            "sku": sale["sku"],
            "financials": {
                "raw_amount": sale["amount"],
                "currency": sale["currency"]
            },
            "status_history": status_history,
            "metadata": {
                "processed_at": datetime.now(timezone.utc),
                "batch_id": self.batch_id
            }
        }


class WriteParquet(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
        self.records = []

    def process(self, record):
        self.records.append(record)

    def finish_bundle(self):
        if not self.records:
            return

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        schema = pa.schema([
            ("id", pa.string()),
            ("store", pa.string()),
            ("sku", pa.string()),
            ("financials", pa.struct([
                ("raw_amount", pa.float64()),
                ("currency", pa.string())
            ])),
            ("status_history", pa.list_(pa.struct([
                ("status", pa.string()),
                ("date", pa.timestamp("us"))
            ]))),
            ("metadata", pa.struct([
                ("processed_at", pa.timestamp("us")),
                ("batch_id", pa.string())
            ]))
        ])

        table = pa.Table.from_pylist(self.records, schema=schema)
        pq.write_table(table, self.output_path)


def format_rejected(row):
    return f'{row["source"]},{row["reason"]},"{row["raw_record"]}"'


def run(args):
    batch_id = str(uuid.uuid4())

    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/rejected", exist_ok=True)

    with beam.Pipeline() as p:
        sales_raw = (
            p
            | "Leer sales_data.csv" >> beam.io.ReadFromText(
                args.sales_path,
                skip_header_lines=1
            )
        )

        logs_raw = (
            p
            | "Leer status_logs.csv" >> beam.io.ReadFromText(
                args.logs_path,
                skip_header_lines=1
            )
        )

        parsed_sales = (
            sales_raw
            | "Parsear ventas" >> beam.ParDo(ParseSales()).with_outputs(
                ParseSales.REJECTED,
                main="valid"
            )
        )

        parsed_logs = (
            logs_raw
            | "Parsear estados" >> beam.ParDo(ParseStatusLogs()).with_outputs(
                ParseStatusLogs.REJECTED,
                main="valid"
            )
        )

        (
            (parsed_sales.rejected, parsed_logs.rejected)
            | "Unir rechazados" >> beam.Flatten()
            | "Formatear rechazados" >> beam.Map(format_rejected)
            | "Guardar rejected_sales.csv" >> beam.io.WriteToText(
                args.rejected_path,
                file_name_suffix=".csv",
                header="source,reason,raw_record"
            )
        )

        grouped = (
            {
                "sales": parsed_sales.valid,
                "logs": parsed_logs.valid
            }
            | "Join por transaction_id" >> beam.CoGroupByKey()
        )

        (
            grouped
            | "Crear estructura anidada" >> beam.ParDo(BuildNestedRecord(batch_id))
            | "Guardar Parquet Silver" >> beam.ParDo(WriteParquet(args.output_path))
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--sales-path", default="data/sales_data.csv")
    parser.add_argument("--logs-path", default="data/status_logs.csv")
    parser.add_argument("--output-path", default="data/silver/sales_silver.parquet")
    parser.add_argument("--rejected-path", default="data/rejected/rejected_sales")
    

    run(parser.parse_args())