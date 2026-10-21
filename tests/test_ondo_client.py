import hashlib
import hmac
import sys
from decimal import Decimal
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from exchanges.ondo import (
    align_to_step,
    build_request_path,
    first_level_price,
    map_order_status,
    market_id_for_ticker,
    sign_ondo_request,
    sign_ondo_ws_login,
)


def test_request_signature_matches_documented_concatenation():
    secret = "ondoApiSecret_SECRET"
    timestamp = "1710000000000"
    method = "GET"
    path = "/v1/perps/orders?market=AAPL-USD.P&limit=1000"
    body = ""
    message = f"{timestamp}{method}{path}{body}"
    expected = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    assert sign_ondo_request(secret, timestamp, method, path, body) == expected


def test_ws_login_signature_uses_fixed_payload():
    secret = "ondoApiSecret_SECRET"
    timestamp = "1710000000000"
    message = f"{timestamp}ondo_perps_ws_login"
    expected = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    assert sign_ondo_ws_login(secret, timestamp) == expected


def test_market_id_for_ticker():
    assert market_id_for_ticker("aapl") == "AAPL-USD.P"
    assert market_id_for_ticker("ETH-USD") == "ETH-USD.P"
    assert market_id_for_ticker("NVDA-USD.P") == "NVDA-USD.P"


def test_status_mapping():
    assert map_order_status("fullyfilled", Decimal("1"), Decimal("1")) == "FILLED"
    assert map_order_status("canceled", Decimal("1"), Decimal("0")) == "CANCELED"
    assert map_order_status("open", Decimal("1"), Decimal("0")) == "OPEN"
    assert map_order_status("open", Decimal("1"), Decimal("0.4")) == "PARTIALLY_FILLED"


def test_book_and_step_helpers():
    assert first_level_price([["339.85", "10"]]) == Decimal("339.85")
    assert first_level_price(["227.60", "80.0"]) == Decimal("227.60")
    assert align_to_step(Decimal("1.237"), Decimal("0.01")) == Decimal("1.24")
    assert build_request_path("/v1/perps/depth", {"market": "AAPL-USD.P", "depth": 1}) == (
        "/v1/perps/depth?market=AAPL-USD.P&depth=1"
    )


if __name__ == "__main__":
    test_request_signature_matches_documented_concatenation()
    test_ws_login_signature_uses_fixed_payload()
    test_market_id_for_ticker()
    test_status_mapping()
    test_book_and_step_helpers()
    print("ok")
