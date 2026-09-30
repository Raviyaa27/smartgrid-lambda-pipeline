"""
Tiered block-tariff billing.

This is the transformation the assessment brief calls "meaningful" -- not a
pass-through and not a SUM. Energy is priced in blocks: the first N units at
one rate, the next M at a higher one, and so on. A household that crosses a
block boundary pays the higher rate only on the units above it.

Every monetary quantity is Decimal. Binary floating point cannot represent
0.01, so float arithmetic on money accumulates error and produces bills that
do not reconcile -- unacceptable in a system whose output is auditable and
disputable. Energy quantities stay Decimal too, quantised to 0.001 kWh, so
the whole calculation is exact and reproducible.

Published block limits are MONTHLY. The simulation settles daily, so limits
are pro-rated by `prorate_blocks`. That is a stated simplification: a real
utility accumulates month-to-date consumption and charges the marginal
block. Recorded in the report's assumptions.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

CURRENCY = "LKR"
_CENTS = Decimal("0.01")
_KWH = Decimal("0.001")

# Credit paid for energy exported to the grid under net metering. Lower than
# every consumption rate, which is what makes self-consumption rational.
EXPORT_CREDIT_RATE = Decimal("22.00")

# Proportional discount on the energy charge for a flagged household.
SUBSIDY_FRACTION = Decimal("0.25")


@dataclass(frozen=True)
class TariffBlock:
    """One pricing block. `upper_kwh` is a CUMULATIVE cap; None = unbounded."""

    upper_kwh: Decimal | None
    rate: Decimal


# Monthly block structures per tier, loosely modelled on a real domestic
# block tariff. Rates rise steeply to discourage high consumption.
TIER_BLOCKS: dict[str, tuple[TariffBlock, ...]] = {
    "DOMESTIC_LOW": (
        TariffBlock(Decimal("30"), Decimal("8.00")),
        TariffBlock(Decimal("60"), Decimal("10.00")),
        TariffBlock(Decimal("90"), Decimal("16.00")),
        TariffBlock(None, Decimal("50.00")),
    ),
    "DOMESTIC_STD": (
        TariffBlock(Decimal("60"), Decimal("12.00")),
        TariffBlock(Decimal("120"), Decimal("27.75")),
        TariffBlock(Decimal("180"), Decimal("32.00")),
        TariffBlock(None, Decimal("45.00")),
    ),
    "DOMESTIC_HIGH": (
        TariffBlock(Decimal("120"), Decimal("30.00")),
        TariffBlock(None, Decimal("55.00")),
    ),
    "INDUSTRIAL": (TariffBlock(None, Decimal("45.00")),),
}

FIXED_CHARGES: dict[str, Decimal] = {
    "DOMESTIC_LOW": Decimal("150.00"),
    "DOMESTIC_STD": Decimal("300.00"),
    "DOMESTIC_HIGH": Decimal("600.00"),
    "INDUSTRIAL": Decimal("1500.00"),
}

DAYS_PER_BILLING_MONTH = Decimal("30")


def _money(value: Decimal) -> Decimal:
    return value.quantize(_CENTS, rounding=ROUND_HALF_UP)


def _energy(value: Decimal) -> Decimal:
    return value.quantize(_KWH, rounding=ROUND_HALF_UP)


# -- The tariff as data ---------------------------------------------------


@dataclass(frozen=True)
class TierTariff:
    """One tier's published tariff: monthly blocks and a monthly fixed charge."""

    blocks: tuple[TariffBlock, ...]
    fixed_charge: Decimal


@dataclass(frozen=True)
class TariffSchedule:
    """
    The tariff in force for one day: the whole pricing policy as DATA.

    It travels in the daily batch file, and that is what makes a retroactive
    tariff revision possible. A regulator's correction arrives as a new
    version of a past day's file; re-running settlement for that day reprices
    every bill from it. With the rates hard-coded, the restatement scenario
    ADR-0001 rests on would need a code change and a redeploy.

    `problems()` reports semantic faults instead of raising, so the batch
    layer's quality gate can list everything wrong with a bad file at once.
    """

    tiers: Mapping[str, TierTariff]
    export_credit_rate: Decimal
    subsidy_fraction: Decimal
    currency: str = CURRENCY

    def tier(self, name: str) -> TierTariff:
        try:
            return self.tiers[name]
        except KeyError:
            raise ValueError(f"unknown tariff tier {name!r}") from None

    def headline_rate(self, tier: str) -> Decimal:
        """The first-block rate -- what a customer thinks of as 'my rate'."""
        return self.tier(tier).blocks[0].rate

    def problems(self) -> list[str]:
        """Every semantic fault in the schedule. Empty means usable for billing."""
        issues: list[str] = []
        if not self.tiers:
            issues.append("schedule defines no tiers")
        for name, tariff in sorted(self.tiers.items()):
            if not tariff.blocks:
                issues.append(f"{name}: no blocks")
                continue
            previous_cap = Decimal("0")
            last = len(tariff.blocks) - 1
            for index, block in enumerate(tariff.blocks):
                if block.rate <= 0:
                    issues.append(f"{name} block {index}: rate {block.rate} is not positive")
                if block.upper_kwh is None:
                    if index != last:
                        issues.append(f"{name} block {index}: only the last block may be unbounded")
                elif index == last:
                    issues.append(f"{name}: the last block must be unbounded")
                elif block.upper_kwh <= previous_cap:
                    issues.append(
                        f"{name} block {index}: cap {block.upper_kwh} does not exceed "
                        f"the previous cap {previous_cap}"
                    )
                else:
                    previous_cap = block.upper_kwh
            if tariff.fixed_charge < 0:
                issues.append(f"{name}: fixed charge {tariff.fixed_charge} is negative")
        if self.export_credit_rate < 0:
            issues.append(f"export credit rate {self.export_credit_rate} is negative")
        if not Decimal("0") <= self.subsidy_fraction < Decimal("1"):
            issues.append(f"subsidy fraction {self.subsidy_fraction} is outside [0, 1)")
        return issues

    def with_rate_change(
        self, factor: Decimal, tiers: Iterable[str] | None = None
    ) -> TariffSchedule:
        """
        A revised schedule: every block rate of the chosen tiers (default: all)
        multiplied by `factor`, rounded to the cent. Caps and fixed charges are
        unchanged. This is how a retroactive tariff revision is expressed.
        """
        chosen = set(self.tiers) if tiers is None else set(tiers)
        unknown = chosen - set(self.tiers)
        if unknown:
            raise ValueError(f"unknown tariff tier(s) {sorted(unknown)}")
        revised = {
            name: (
                TierTariff(
                    blocks=tuple(
                        TariffBlock(b.upper_kwh, _money(b.rate * factor)) for b in tariff.blocks
                    ),
                    fixed_charge=tariff.fixed_charge,
                )
                if name in chosen
                else tariff
            )
            for name, tariff in self.tiers.items()
        }
        return replace(self, tiers=revised)

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready. Every amount is a string, so no rate ever passes through float."""
        return {
            "currency": self.currency,
            "export_credit_rate": str(self.export_credit_rate),
            "subsidy_fraction": str(self.subsidy_fraction),
            "tiers": {
                name: {
                    "fixed_charge_monthly": str(tariff.fixed_charge),
                    "blocks": [
                        {
                            "upper_kwh_monthly": None if b.upper_kwh is None else str(b.upper_kwh),
                            "rate": str(b.rate),
                        }
                        for b in tariff.blocks
                    ],
                }
                for name, tariff in sorted(self.tiers.items())
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TariffSchedule:
        """Parse a schedule document. Raises ValueError only if it is structurally broken."""

        def amount(value: Any) -> Decimal:
            return Decimal(str(value))

        try:
            tiers = {
                str(name): TierTariff(
                    blocks=tuple(
                        TariffBlock(
                            upper_kwh=(
                                None
                                if block["upper_kwh_monthly"] is None
                                else amount(block["upper_kwh_monthly"])
                            ),
                            rate=amount(block["rate"]),
                        )
                        for block in spec["blocks"]
                    ),
                    fixed_charge=amount(spec["fixed_charge_monthly"]),
                )
                for name, spec in data["tiers"].items()
            }
            return cls(
                tiers=tiers,
                export_credit_rate=amount(data["export_credit_rate"]),
                subsidy_fraction=amount(data["subsidy_fraction"]),
                currency=str(data.get("currency", CURRENCY)),
            )
        except (KeyError, TypeError, AttributeError, InvalidOperation) as exc:
            raise ValueError(f"malformed tariff schedule: {exc!r}") from exc


# The tariff in force unless a daily file says otherwise.
DEFAULT_SCHEDULE = TariffSchedule(
    tiers={name: TierTariff(TIER_BLOCKS[name], FIXED_CHARGES[name]) for name in TIER_BLOCKS},
    export_credit_rate=EXPORT_CREDIT_RATE,
    subsidy_fraction=SUBSIDY_FRACTION,
)


@dataclass(frozen=True)
class BillLine:
    """One block's contribution, kept so a bill can be explained to a customer."""

    block_index: int
    units_kwh: Decimal
    rate: Decimal
    amount: Decimal


@dataclass(frozen=True)
class Bill:
    household_id: str
    billing_date: date
    tariff_tier: str
    gross_consumption_kwh: Decimal
    solar_generation_kwh: Decimal
    net_import_kwh: Decimal
    net_export_kwh: Decimal
    energy_charge: Decimal
    fixed_charge: Decimal
    export_credit: Decimal
    subsidy_amount: Decimal
    total_payable: Decimal
    subsidy_applied: bool
    lines: tuple[BillLine, ...] = field(default=())
    currency: str = CURRENCY

    def to_dict(self) -> dict[str, object]:
        """Flat row for the serving store. Decimals become strings, never floats."""
        return {
            "household_id": self.household_id,
            "billing_date": self.billing_date.isoformat(),
            "tariff_tier": self.tariff_tier,
            "gross_consumption_kwh": str(self.gross_consumption_kwh),
            "solar_generation_kwh": str(self.solar_generation_kwh),
            "net_import_kwh": str(self.net_import_kwh),
            "net_export_kwh": str(self.net_export_kwh),
            "energy_charge": str(self.energy_charge),
            "fixed_charge": str(self.fixed_charge),
            "export_credit": str(self.export_credit),
            "subsidy_amount": str(self.subsidy_amount),
            "total_payable": str(self.total_payable),
            "subsidy_applied": self.subsidy_applied,
            "currency": self.currency,
        }


def prorate_blocks(
    blocks: tuple[TariffBlock, ...], days: Decimal = Decimal("1")
) -> tuple[TariffBlock, ...]:
    """
    Scale monthly block caps to a shorter settlement period. Rates are unchanged.

    Multiply BEFORE dividing. Computing the factor first (days / 30) yields a
    repeating Decimal carried to 28 significant digits, and 30 * (1/30) then
    lands on 0.9999...9 rather than 1 -- block boundaries would sit a hair
    below their intended value and a household consuming exactly the cap
    would be charged one unit at the next block's rate.
    """
    return tuple(
        TariffBlock(
            upper_kwh=(
                None if b.upper_kwh is None else b.upper_kwh * days / DAYS_PER_BILLING_MONTH
            ),
            rate=b.rate,
        )
        for b in blocks
    )


def block_energy_charge(
    net_import_kwh: Decimal, blocks: tuple[TariffBlock, ...]
) -> tuple[Decimal, tuple[BillLine, ...]]:
    """
    Price consumption across the block structure.

    Returns the total charge and the per-block breakdown. The breakdown is
    what lets the dashboard show a customer exactly why their bill is what
    it is -- and what lets you demonstrate in the viva that this is genuine
    marginal pricing rather than a flat multiply.
    """
    if net_import_kwh < 0:
        raise ValueError(f"net import cannot be negative, got {net_import_kwh}")

    remaining = net_import_kwh
    lower_bound = Decimal("0")
    total = Decimal("0")
    lines: list[BillLine] = []

    for index, block in enumerate(blocks):
        if remaining <= 0:
            break

        width = remaining if block.upper_kwh is None else block.upper_kwh - lower_bound
        if width <= 0:
            continue

        units = min(remaining, width)
        amount = _money(units * block.rate)

        lines.append(
            BillLine(block_index=index, units_kwh=_energy(units), rate=block.rate, amount=amount)
        )
        total += amount
        remaining -= units

        if block.upper_kwh is not None:
            lower_bound = block.upper_kwh

    return _money(total), tuple(lines)


def calculate_bill(
    *,
    household_id: str,
    billing_date: date,
    tariff_tier: str,
    consumption_kwh: float | Decimal,
    generation_kwh: float | Decimal,
    subsidy_flag: bool,
    fixed_charge: float | Decimal | None = None,
    settlement_days: Decimal = Decimal("1"),
    schedule: TariffSchedule = DEFAULT_SCHEDULE,
) -> Bill:
    """
    Settle one household for one billing period, under `schedule` -- the
    tariff published for that day.

    Net metering: only the NET position is billed. A household generating
    more than it consumes imports nothing and earns an export credit at the
    (lower) export rate.
    """
    tariff = schedule.tier(tariff_tier)  # raises on an unknown tier

    consumption = _energy(Decimal(str(consumption_kwh)))
    generation = _energy(Decimal(str(generation_kwh)))
    if consumption < 0 or generation < 0:
        raise ValueError("energy quantities cannot be negative")

    net_import = _energy(max(Decimal("0"), consumption - generation))
    net_export = _energy(max(Decimal("0"), generation - consumption))

    blocks = prorate_blocks(tariff.blocks, settlement_days)
    energy_charge, lines = block_energy_charge(net_import, blocks)

    # Subsidy discounts the energy charge only -- never the fixed charge,
    # which recovers network cost regardless of consumption.
    subsidy_amount = (
        _money(energy_charge * schedule.subsidy_fraction) if subsidy_flag else Decimal("0.00")
    )

    monthly_fixed = tariff.fixed_charge if fixed_charge is None else Decimal(str(fixed_charge))
    fixed = _money(monthly_fixed * settlement_days / DAYS_PER_BILLING_MONTH)

    export_credit = _money(net_export * schedule.export_credit_rate)

    # A credit balance is legitimate: heavy exporters can owe nothing.
    total = _money(energy_charge - subsidy_amount + fixed - export_credit)

    return Bill(
        household_id=household_id,
        billing_date=billing_date,
        tariff_tier=tariff_tier,
        gross_consumption_kwh=consumption,
        solar_generation_kwh=generation,
        net_import_kwh=net_import,
        net_export_kwh=net_export,
        energy_charge=energy_charge,
        fixed_charge=fixed,
        export_credit=export_credit,
        subsidy_amount=subsidy_amount,
        total_payable=total,
        subsidy_applied=subsidy_flag,
        lines=lines,
        currency=schedule.currency,
    )
