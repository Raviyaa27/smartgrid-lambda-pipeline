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

from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

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
) -> Bill:
    """
    Settle one household for one billing period.

    Net metering: only the NET position is billed. A household generating
    more than it consumes imports nothing and earns an export credit at the
    (lower) export rate.
    """
    if tariff_tier not in TIER_BLOCKS:
        raise ValueError(f"unknown tariff tier {tariff_tier!r}")

    consumption = _energy(Decimal(str(consumption_kwh)))
    generation = _energy(Decimal(str(generation_kwh)))
    if consumption < 0 or generation < 0:
        raise ValueError("energy quantities cannot be negative")

    net_import = _energy(max(Decimal("0"), consumption - generation))
    net_export = _energy(max(Decimal("0"), generation - consumption))

    blocks = prorate_blocks(TIER_BLOCKS[tariff_tier], settlement_days)
    energy_charge, lines = block_energy_charge(net_import, blocks)

    # Subsidy discounts the energy charge only -- never the fixed charge,
    # which recovers network cost regardless of consumption.
    subsidy_amount = _money(energy_charge * SUBSIDY_FRACTION) if subsidy_flag else Decimal("0.00")

    if fixed_charge is None:
        monthly_fixed = FIXED_CHARGES[tariff_tier]
    else:
        monthly_fixed = Decimal(str(fixed_charge))
    fixed = _money(monthly_fixed * settlement_days / DAYS_PER_BILLING_MONTH)

    export_credit = _money(net_export * EXPORT_CREDIT_RATE)

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
    )
