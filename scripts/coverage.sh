#!/usr/bin/env bash
# Enforce workspace and per-crate line coverage thresholds using cargo-llvm-cov.
set -euo pipefail

# Per-crate minimum line coverage percentages.
MIN_TOTAL=90
MIN_OPENHOME_CORE=92
MIN_OPENHOME_SERVER=90
MIN_OPENHOME_CLIENT=90
MIN_OPENHOME_SONGCAST=92
LOW_FILE_WARN=85

tmp_json="$(mktemp -t openhome-cov.XXXXXX.json)"
trap 'rm -f "$tmp_json"' EXIT

# Exclude hardware-only cpal_sink.rs (requires a physical OS audio device) from headless coverage.
cargo llvm-cov --workspace --ignore-filename-regex 'cpal_sink\.rs$' --json --output-path "$tmp_json"

jq -r \
  --argjson min_total "$MIN_TOTAL" \
  --argjson min_core "$MIN_OPENHOME_CORE" \
  --argjson min_server "$MIN_OPENHOME_SERVER" \
  --argjson min_client "$MIN_OPENHOME_CLIENT" \
  --argjson min_songcast "$MIN_OPENHOME_SONGCAST" \
  --argjson low_file_warn "$LOW_FILE_WARN" '
  def pct($cov; $tot):
    if $tot == 0 then 100.0 else (($cov * 10000 / $tot) | round) / 100 end;

  def threshold($crate):
    if $crate == "openhome-core" then $min_core
    elif $crate == "openhome-server" then $min_server
    elif $crate == "openhome-client" then $min_client
    elif $crate == "openhome-songcast" then $min_songcast
    else $min_total
    end;

  .data[0] as $d |
  ($d.files
    | map(select(.filename | test("crates/[^/]+/src/")))
    | group_by(.filename | capture("crates/(?<crate>[^/]+)/").crate)
    | map({
        crate: (.[0].filename | capture("crates/(?<crate>[^/]+)/").crate),
        covered: (map(.summary.lines.covered) | add),
        count: (map(.summary.lines.count) | add)
      })
    | map(. + {
        pct: pct(.covered; .count),
        min: threshold(.crate)
      })
    | map(. + { ok: (.pct >= .min) })
  ) as $crates |

  {
    covered: ($crates | map(.covered) | add),
    count: ($crates | map(.count) | add)
  } as $tot_raw |
  ($tot_raw + {
    pct: pct($tot_raw.covered; $tot_raw.count),
    min: $min_total,
    ok: (pct($tot_raw.covered; $tot_raw.count) >= $min_total)
  }) as $total |

  ($d.files
    | map(select(.filename | test("crates/[^/]+/src/")))
    | map({
        file: (.filename | sub("^.*/crates/"; "crates/")),
        pct: pct(.summary.lines.covered; .summary.lines.count),
        missed: (.summary.lines.count - .summary.lines.covered)
      })
    | map(select(.pct < $low_file_warn))
    | sort_by(.pct)
  ) as $low_files |

  (
    ["", "Crate Coverage Summary", "================================================================"]
    + ($crates | map(
        (if .ok then "PASS" else "FAIL" end) + "  " +
        (.crate + "                    ")[0:20] + "  " +
        ("\(.pct)%" + "        ")[0:8] + "  " +
        "(min \(.min)%, \(.covered)/\(.count) lines)"
      ))
    + ["----------------------------------------------------------------"]
    + [
        (if $total.ok then "PASS" else "FAIL" end) + "  " +
        ("WORKSPACE TOTAL     ") + "  " +
        ("\($total.pct)%" + "        ")[0:8] + "  " +
        "(min \($total.min)%, \($total.covered)/\($total.count) lines)"
      ]
    + (if ($low_files | length) > 0 then
        ["", "Files below \($low_file_warn)% line coverage (informational):"]
        + ($low_files | map("  \(.pct)%  \(.file) (\(.missed) missed lines)"))
      else [] end)
  )[]
' "$tmp_json"

# Exit non-zero if workspace total or any crate missed its threshold.
jq -e \
  --argjson min_total "$MIN_TOTAL" \
  --argjson min_core "$MIN_OPENHOME_CORE" \
  --argjson min_server "$MIN_OPENHOME_SERVER" \
  --argjson min_client "$MIN_OPENHOME_CLIENT" \
  --argjson min_songcast "$MIN_OPENHOME_SONGCAST" '
  def pct($cov; $tot):
    if $tot == 0 then 100.0 else (($cov * 10000 / $tot) | round) / 100 end;

  def threshold($crate):
    if $crate == "openhome-core" then $min_core
    elif $crate == "openhome-server" then $min_server
    elif $crate == "openhome-client" then $min_client
    elif $crate == "openhome-songcast" then $min_songcast
    else $min_total
    end;

  .data[0].files
  | map(select(.filename | test("crates/[^/]+/src/")))
  | group_by(.filename | capture("crates/(?<crate>[^/]+)/").crate)
  | map({
      crate: (.[0].filename | capture("crates/(?<crate>[^/]+)/").crate),
      covered: (map(.summary.lines.covered) | add),
      count: (map(.summary.lines.count) | add)
    })
  | (all(pct(.covered; .count) >= threshold(.crate)))
    and (pct((map(.covered) | add); (map(.count) | add)) >= $min_total)
' "$tmp_json" >/dev/null
