import datetime as dt
import dateutil.parser

vcxt_trades_columns = [
    "created_at",
    "datetime",
    "trade_id",
    "timestamp",
    "exchange",
    "symbol",
    "price",
    "amount"
]


def _now_ts():
    return dt.datetime.now(tz=dt.timezone.utc).timestamp()


def parse_date(date: str):
    return dateutil.parser.parse(date) if date is not None else date


def clean_and_transform(json_data: dict) -> dict:
    json_data["created_at"] = dt.datetime.now(tz=dt.timezone.utc)

    explicit_cast = {
        "trade_id": str,
        "exchange": str,
        "symbol": str,
        "price": float,
        "amount": float,
        "timestamp": int,
        "datetime": parse_date
    }

    for field, cast in explicit_cast.items():
        if json_data.get(field):
            json_data[field] = cast(json_data[field])

    for field in list(json_data.keys()):
        if field not in vcxt_trades_columns:
            del json_data[field]

    for field in vcxt_trades_columns:
        if field not in json_data:
            json_data[field] = None

    return json_data
