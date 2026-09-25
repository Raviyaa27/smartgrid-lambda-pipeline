# ADR-0004: One shared transformation module for both Lambda layers

- **Status:** Accepted
- **Date:** 2026-09-20
- **Deciders:** Project team

## Context

[ADR-0001](0001-lambda-over-kappa.md) accepts Lambda's central cost: two
processing paths that must agree. The canonical objection, stated most
clearly in Jay Kreps' *Questioning the Lambda Architecture* (2014), is that
this agreement is not sustainable — the same business logic written twice,
in two engines, by people under deadline, diverges. And when it diverges, the
symptom is the worst possible one: a number that changes when a day is
settled, with no error anywhere to explain why.

The objection is correct as stated. A decision to adopt Lambda is only
defensible if it comes with a structural answer, not a promise of
discipline. Code review and good intentions are not an architecture.

## Decision

**Declare the rules once as data; apply them through thin adapters; import
the same module into both layers.**

Three parts:

1. **Rules as data.** Every field the pipeline handles is declared in a
   `FieldSpec` table in `src/smartgrid/common/schemas.py` — its type, whether
   it is required, and its valid range. These tables are the single source of
   truth. Nothing else states what a valid reading is.

2. **Thin adapters over those rules.** `src/smartgrid/common/transformations.py`
   exposes two validators, both of which walk the same `FieldSpec` table:

   - `validate_record(dict)` — row-at-a-time, pure Python, used by producers
     and tests.
   - `validate_frame(pandas.DataFrame)` — vectorised, used from a Spark
     pandas UDF in both the speed and batch layers.

   Neither contains rules of its own. Adding a constraint means editing the
   table, which changes both validators simultaneously or neither.

3. **Drift fails the build.** `tests/unit/test_transformations.py` asserts
   that the two validators return identical verdicts *and* identical
   quarantine reasons for the same input. If the adapters ever disagree, CI
   goes red before the divergence can reach a bill.

Enrichment (`enrich_reading`) and the billing calculation
(`src/smartgrid/common/billing.py`) live in the same shared package for the
same reason. `net_kwh`'s sign convention is defined exactly once, so the
real-time dashboard and the settlement run cannot disagree about what
"exporting" means.

## Alternatives considered

### Write each layer independently and rely on review

Rejected. This is the practice Kreps' objection describes, and it fails for
structural reasons rather than personal ones: the two layers are edited at
different times, for different reasons, often by different people, and
nothing forces a change in one to be reflected in the other. Under deadline
it fails reliably.

### Express the rules as Spark SQL and have Python call Spark for everything

**What it gives.** Native Catalyst execution, fast, one dialect.

**Why rejected.** It forces a Spark session into the producers and unit
tests, which currently run in milliseconds as plain Python. It would make the
fast feedback loop slow, and slow tests get skipped.

### Generate both Python predicates and Spark Column expressions from the spec

**What it gives.** The ideal: one declaration, two *native* implementations,
no interpretation overhead in either.

**Why rejected — for now.** This is genuinely the right answer at production
scale and is recorded as future work in the report's limitations. It was
rejected for this project on effort: a correct expression compiler is a
meaningful subsystem, and the pandas-UDF route buys the same *correctness*
guarantee immediately. We chose the version that is provably correct now
over the version that is also fast but might not be finished.

## Consequences

**Positive**

- Lambda's headline weakness is addressed by construction. The two layers
  cannot hold different beliefs about validity, because there is only one
  place where validity is defined.
- The claim is testable, and tested. "Our layers agree" is an assertion that
  CI checks on every commit rather than a paragraph in a report.
- Adding a rule is a one-line edit to a table.
- The rules are readable as data, which makes the report's description of
  them accurate rather than approximate.

**Negative**

- **Spark executes Python, not Catalyst.** A pandas UDF serialises batches to
  Python and back; native column expressions would be substantially faster.
  This is a real throughput cost, paid deliberately.
- The `FieldSpec` indirection is one more concept to understand than a
  straightforward `if` chain.
- The convenience dataclasses in `schemas.py` restate field *names*
  alongside the spec tables, which is a second, smaller drift surface —
  mitigated by a test asserting the two sets match, but not eliminated.

**Mitigations**

- The validators are vectorised over pandas rather than invoked per row, so
  the overhead is per micro-batch rather than per record.
- Throughput is measured and reported. If the UDF ever becomes the
  bottleneck, the expression-compilation alternative above is the planned
  route, and the `FieldSpec` table is already the correct input for it.

## Revisit if

- Measured throughput shows pandas-UDF overhead dominating the speed layer's
  latency budget — build the expression compiler.
- The batch layer is removed (ADR-0001's revisit conditions), which removes
  the drift risk this record exists to address and makes the indirection
  unjustified complexity.
- The rule set grows beyond what a flat `FieldSpec` table can express — for
  instance, cross-field or stateful constraints — at which point the
  declaration format needs redesigning rather than extending.
