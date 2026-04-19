from __future__ import annotations

import pytest
from pydantic import ValidationError

from polymarket.models import GammaMarket, PricePoint, TokenInfo, detect_cancelled


VALID_MARKET = {
    "id": "123",
    "question": "Will the Lakers win?",
    "slug": "lakers-win",
    "active": True,
    "closed": False,
    "liquidity": "1500.50",
    "volume": "3200.00",
    "conditionId": "0xabc",
    "endDateIso": "2025-04-10T00:00:00Z",
    "clobTokenIds": ["token-yes", "token-no"],
}


class TestGammaMarket:
    def test_happy_path_with_aliases(self) -> None:
        m = GammaMarket.model_validate(VALID_MARKET)
        assert m.id == "123"
        assert m.condition_id == "0xabc"
        assert m.end_date_iso == "2025-04-10T00:00:00Z"
        assert m.clob_token_ids == ["token-yes", "token-no"]

    def test_coerce_numeric_from_string(self) -> None:
        m = GammaMarket.model_validate(VALID_MARKET)
        assert m.liquidity == 1500.50
        assert m.volume == 3200.00

    def test_coerce_numeric_none_becomes_zero(self) -> None:
        data = {**VALID_MARKET, "liquidity": None, "volume": None}
        m = GammaMarket.model_validate(data)
        assert m.liquidity == 0.0
        assert m.volume == 0.0

    def test_end_date_iso_optional(self) -> None:
        data = {k: v for k, v in VALID_MARKET.items() if k != "endDateIso"}
        m = GammaMarket.model_validate(data)
        assert m.end_date_iso is None

    def test_clob_token_ids_defaults_to_empty(self) -> None:
        data = {k: v for k, v in VALID_MARKET.items() if k != "clobTokenIds"}
        m = GammaMarket.model_validate(data)
        assert m.clob_token_ids == []

    def test_missing_required_field_raises(self) -> None:
        data = {k: v for k, v in VALID_MARKET.items() if k != "conditionId"}
        with pytest.raises(ValidationError):
            GammaMarket.model_validate(data)

    def test_populate_by_name(self) -> None:
        # Should also work when using Python field names directly
        data = {**VALID_MARKET, "condition_id": "0xdef"}
        data.pop("conditionId")
        m = GammaMarket.model_validate(data)
        assert m.condition_id == "0xdef"


class TestPricePoint:
    def test_valid_construction(self) -> None:
        pp = PricePoint(t=1700000000, p=0.72)
        assert pp.t == 1700000000
        assert pp.p == 0.72

    def test_p_coerced_from_int(self) -> None:
        pp = PricePoint(t=1700000000, p=1)
        assert pp.p == 1.0

    def test_invalid_p_type_raises(self) -> None:
        with pytest.raises(ValidationError):
            PricePoint(t=1700000000, p="not-a-number")

    def test_missing_t_raises(self) -> None:
        with pytest.raises(ValidationError):
            PricePoint(p=0.5)  # type: ignore[call-arg]

    def test_missing_p_raises(self) -> None:
        with pytest.raises(ValidationError):
            PricePoint(t=1700000000)  # type: ignore[call-arg]


class TestTokenInfo:
    def test_valid_with_team_name(self) -> None:
        ti = TokenInfo(
            token_id="tok-yes",
            outcome="Yes",
            team_name="Lakers",
            market_slug="lakers-win",
            condition_id="0xabc",
        )
        assert ti.team_name == "Lakers"

    def test_team_name_optional(self) -> None:
        ti = TokenInfo(
            token_id="tok-yes",
            outcome="Yes",
            market_slug="lakers-win",
            condition_id="0xabc",
        )
        assert ti.team_name is None

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            TokenInfo(outcome="Yes", market_slug="lakers-win", condition_id="0xabc")  # type: ignore[call-arg]


class TestDetectCancelled:
    def _pts(self, *prices: float) -> list[PricePoint]:
        return [PricePoint(t=i, p=p) for i, p in enumerate(prices)]

    def test_all_at_0_5_returns_true(self) -> None:
        assert detect_cancelled(self._pts(0.5, 0.5, 0.5)) is True

    def test_all_within_threshold_returns_true(self) -> None:
        assert detect_cancelled(self._pts(0.5001, 0.4999, 0.5)) is True

    def test_one_point_outside_threshold_returns_false(self) -> None:
        assert detect_cancelled(self._pts(0.5, 0.5, 0.7)) is False

    def test_single_point_returns_false(self) -> None:
        assert detect_cancelled(self._pts(0.5)) is False

    def test_empty_list_returns_false(self) -> None:
        assert detect_cancelled([]) is False

    def test_normal_price_history_returns_false(self) -> None:
        assert detect_cancelled(self._pts(0.65, 0.70, 0.68, 0.72)) is False

    def test_exact_threshold_boundary_returns_false(self) -> None:
        # |p - 0.5| == 0.001 is NOT within threshold (strict less-than)
        assert detect_cancelled(self._pts(0.501, 0.501)) is False
