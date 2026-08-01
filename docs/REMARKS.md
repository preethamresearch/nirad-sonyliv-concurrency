# Demo review remarks — 1 Aug 2026

Captured live while driving the dashboard at `localhost:877` in Chrome.
Source: user review of the running demo. Not yet actioned.

## 1. No landing page; not on ClickHouse brand guidelines

The demo opens straight into a chart. There is no entry point that frames
the problem, the team, or the result. Styling is ad-hoc, not ClickHouse's
visual language.

## 2. Analytics is single-dimensional

Present: `platform`, `country`, `type` — three single-select dropdowns that
AND together. Missing: multi-select within a dimension, content/title-level
breakdown, and any notion of *who is asking*.

Audiences to design for, in priority order:

| Audience | What they need to answer |
|---|---|
| Video Ops | Is something wrong right now, where, and on which platform/CDN? |
| Video Analyst | How did this title/event actually perform vs the naive number? |
| Product Management | Which cohorts/platforms/geos are engaging, and what is the trend? |
| SRE / engineering | *Later.* Query cost, freshness, pipeline health. |

## 3. Should be multiple dashboards, not one page

One page showing one metric is an engineer's view. Needs to be a set of
purpose-built dashboards, navigable.

## 4. Production-ready UX, Datadog-grade

Target the interaction model of Datadog's data-analysis pages:

- **Time range as a first-class control** — presets (15m / 1h / 4h / 1d /
  full range) plus custom absolute range; every graph and table on the page
  bound to it.
- **Multiple graph types** shown together, driven by dimension
  combinations — not a single line chart.
- **A data table** alongside the graphs, sorted by the metric that matters,
  showing the per-dimension rows behind the curve.
- **Progressive narrowing** — click a series, a legend entry, a table row,
  or drag-select a time span, and the whole page re-scopes to that filter.
  Filters accumulate as removable chips. This is the core of the ask: the
  user narrows down *as they go*, without editing a query.

## Known rough edge (found during this run, not a user remark)

Screenshot/render capture stalls ~30s after each interaction while the SVG
redraws. Recovers on retry, but on a projector it reads as a hang after
every click. Redraw needs to be incremental or debounced.
