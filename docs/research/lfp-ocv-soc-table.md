# LFP OCV-to-SoC table for the rested anchor

Research for [#10](https://github.com/Kylevdm/bms-flux/issues/10), which feeds the rested OCV anchor in [#7](https://github.com/Kylevdm/bms-flux/issues/7).

The rule under test: a pack is at rest after |I| < 0.02 C for 2 h. bms-flux maps the average cell voltage to a true SoC through a built-in table, and trusts the result only when it is < 20 % or > 90 %. It raises a drift finding when |OCV SoC − reported SoC| > 10 points. The rule is skipped below a 10 °C minimum cell temperature.

## TL;DR

- **Cell makers do not publish OCV-SoC tables.** The EVE LF280K, LF280N, LF280, LF304 and MB31 (314 Ah) specs and the Hithium 280 Ah spec have no OCV-SoC table or curve. The only OCV-like datum is a shipping voltage. The default table therefore has to come from published measurements.
- **Proposed table:** the 25 °C midpoint (mean of the charge and discharge branches) of CALCE's low-current OCV tests on two A123 LFP/graphite cells (below). It is dense at both ends.
- **Low end (< 20 %): the rule holds.** At 10–20 % SoC the curve rises about 3–4 mV per SoC point. True rested hysteresis is about ±10 mV, and the 10 °C-vs-25 °C shift is about 5 mV. Together they put the OCV SoC within about ±4 points, which is well inside a 10-point margin.
- **High end (> 90 %): the rule does not hold as written.** From 80 % to 92 % SoC the curve rises only about 9 mV in total (< 1 mV per point). The ±10 mV hysteresis alone can move a reading anywhere between about 75 % and 95 %. A charge-rested pack at a true 85 % can read as 93–94 %. **Recommendation: trust the upper end only when OCV SoC ≥ 97 % (cell average ≥ 3.39 V rested), or drop the upper rested anchor and leave the top end to the full-charge anchor.**
- **The 10 °C gate holds.** From 25 °C down to 10 °C the OCV itself moves 5–9 mV, which is ≤ 2 SoC points at the low end. Cold mainly slows relaxation and widens the apparent hysteresis: the C/22 half-gap grows from 32 to 47 mV at 10 % SoC, and it roughly doubles again at 0 °C. Below 10 °C a 2 h rest is not enough, so keep the gate.

## 1. What the cell makers publish

| Document | OCV-SoC data? | What it does give |
|---|---|---|
| EVE LF280K Product Specification, File No. LF280K-72174, Ver. A, 2021-01-16 ([pdf](https://akkudoktor.net/uploads/short-url/maYfKTLNOcRNqlYZNRluaz43RTL.pdf)) | **No.** The appendix has only a 25 °C C-rate discharge curve and a discharge curve per temperature, both under load. | Recommended SoC window 10–90 %. Shipped at "3.0~3.3 V (30 ~ 50% SOC)". Keep stored cells above 2.9 V. |
| EVE LF280N spec, LF280N-72174 Ver. A ([pdf](https://akkudoktor.net/uploads/short-url/2vo4dBRggjXOlkkd4vD5pnKzR5w.pdf)) and LF280 spec, LF280-73103 ([pdf](https://community.openenergymonitor.org/uploads/short-url/5rky25pklzJvZvo1QIlGFf4umvc.pdf)) | No | Shipped 30–50 % SoC / "half charged". |
| EVE LF304 spec, PBRI-LF304-D06-01 Ver. E, 2023-06-15 ([pdf](https://cdn.shopify.com/s/files/1/0481/9678/0183/files/PBRI-LF304-D06-01-_LF304_-E_-2023.6.15_new.pdf)) | No. It defines "OCV" but gives no table. | — |
| EVE MB31 (314 Ah) spec, PBRI-MB31-D06-01 Ver. A ([pdf](https://cdn.shopify.com/s/files/1/0763/0562/9462/files/EVE_3.2V_314Ah_LiFePO4_Lithium_Battery_Cell.pdf)) | No | — |
| Hithium 280 Ah cell technical specification, Ver. A/0 ([pdf](https://off-grid-garage.com/wp-content/uploads/2023/01/hithium-280ah-specificaton-20210806-v1.3.pdf)) | No table | **Shipping voltage 3.28–3.30 V "(25±2℃), OCV, 27%SOC"**. This is the only manufacturer OCV point found, and it is used below as a cross-check. |

These are copies on third-party hosts, because EVE and Hithium do not publish specs openly. Of the cells Seplos packs commonly ship with, I found no CATL 280/302 Ah spec in a public, verifiable form.

## 2. Measured data used: CALCE A123 low-current OCV

Source: University of Maryland CALCE battery data, A123 (LiFePO4) "low current OCV" tests ([index](https://calce.umd.edu/battery-data)). The raw Arbin exports were downloaded and analysed for this note:

- `A123_OCV0-20120618.zip`, `A123_OCV10-20120611.zip`, `A123_OCV20-20120614.zip`, `A123_OCV25-20120905.zip` from `https://web.calce.umd.edu/batteries/data/`
- Each zip holds two cells (A1-007, A1-008) with a nominal capacity of 1.1 Ah. A previous user of this data identifies the cells as "LiFePO4 1.1Ah … 3.6V / 2.0V" ([arXiv 2410.23646, Table I](https://arxiv.org/abs/2410.23646)).
- Test schedule, read from the step data: CC-CV charge to 3.6 V, 1 h rest, **discharge at 0.05 A (≈ C/22) to 2.0 V**, 1 h rest, **charge at 0.05 A to 3.6 V**, 1 h rest. The measured chamber temperatures were −0.1…0.4 °C, 10.5–10.8 °C, 21.3–21.7 °C and 26.5–26.8 °C.
- Method: SoC on each branch = Ah throughput ÷ that branch's total Ah. The "mid" curve is (discharge + charge) / 2, the usual pseudo-OCV estimate. The half-gap (charge − discharge) / 2 is an **upper bound** on the rested hysteresis, because at C/22 it also includes resistive and kinetic overpotential.

### 2.1 Pseudo-OCV per cell (mean of the two cells)

| SoC % | mid 25 °C | mid 10 °C | mid 0 °C | 10 °C − 25 °C (mV) | discharge 25 °C | charge 25 °C | half-gap 25 °C (mV) | half-gap 10 °C (mV) | cell-to-cell spread 25 °C (mV) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.794 | 2.789 | 2.709 | −6 | 2.747 | 2.841 | 47 | 87 | 5 |
| 2 | 2.917 | 2.909 | 2.843 | −8 | 2.878 | 2.956 | 39 | 70 | 6 |
| 3 | 2.994 | 2.986 | 2.934 | −8 | 2.958 | 3.030 | 36 | 64 | 7 |
| 5 | 3.098 | 3.090 | 3.057 | −8 | 3.064 | 3.131 | 34 | 56 | 8 |
| 7 | 3.167 | 3.160 | 3.138 | −7 | 3.136 | 3.199 | 32 | 51 | 9 |
| 8 | 3.193 | 3.185 | 3.162 | −7 | 3.161 | 3.224 | 31 | 49 | 7 |
| 10 | 3.209 | 3.203 | 3.184 | −6 | 3.178 | 3.241 | 32 | 47 | 1 |
| 12 | 3.215 | 3.210 | 3.197 | −5 | 3.183 | 3.247 | 32 | 47 | 1 |
| 15 | 3.226 | 3.221 | 3.215 | −5 | 3.194 | 3.258 | 32 | 46 | 3 |
| 17 | 3.237 | 3.232 | 3.226 | −5 | 3.205 | 3.269 | 32 | 45 | 3 |
| 20 | 3.251 | 3.246 | 3.242 | −5 | 3.216 | 3.286 | 35 | 48 | 4 |
| 25 | 3.269 | 3.265 | 3.262 | −4 | 3.234 | 3.304 | 35 | 48 | 4 |
| 30 | 3.283 | 3.279 | 3.276 | −4 | 3.247 | 3.319 | 36 | 49 | 4 |
| 40 | 3.303 | 3.296 | 3.290 | −7 | 3.276 | 3.330 | 27 | 41 | 1 |
| 50 | 3.307 | 3.301 | 3.296 | −6 | 3.281 | 3.332 | 26 | 40 | 1 |
| 60 | 3.310 | 3.305 | 3.302 | −5 | 3.284 | 3.336 | 26 | 40 | 1 |
| 70 | 3.325 | 3.317 | 3.314 | −9 | 3.299 | 3.352 | 26 | 42 | 14 |
| 80 | 3.346 | 3.345 | 3.338 | 0 | 3.323 | 3.368 | 23 | 39 | 2 |
| 85 | 3.348 | 3.349 | 3.344 | +1 | 3.325 | 3.370 | 23 | 39 | 2 |
| 90 | 3.352 | 3.355 | 3.350 | +4 | 3.329 | 3.375 | 23 | 41 | 3 |
| 92 | 3.355 | 3.360 | 3.354 | +5 | 3.331 | 3.379 | 24 | 43 | 4 |
| 94 | 3.361 | 3.368 | 3.361 | +7 | 3.335 | 3.387 | 26 | 47 | 5 |
| 95 | 3.366 | 3.373 | 3.366 | +7 | 3.338 | 3.394 | 28 | 50 | 5 |
| 96 | 3.374 | 3.382 | 3.374 | +8 | 3.342 | 3.405 | 31 | 54 | 6 |
| 97 | 3.386 | 3.395 | 3.387 | +9 | 3.350 | 3.423 | 37 | 61 | 6 |
| 98 | 3.410 | 3.417 | 3.410 | +7 | 3.367 | 3.454 | 44 | 68 | 6 |
| 99 | 3.455 | 3.456 | 3.453 | +1 | 3.403 | 3.507 | 52 | 73 | 3 |
| 99.5 | 3.491 | 3.487 | 3.489 | −4 | 3.436 | 3.547 | 55 | 73 | 1 |

### 2.2 Relaxation after the end steps (cell A1-007, V at minutes after current stops)

| Rest after | T | 5 min | 30 min | 59 min |
|---|---|---|---|---|
| C/22 charge to 3.6 V | 26.8 °C | 3.562 | 3.506 | 3.480 |
| C/22 charge to 3.6 V | 10.6 °C | 3.536 | 3.462 | 3.433 |
| C/22 discharge to 2.0 V | 26.6 °C | 2.304 | 2.421 | 2.480 |
| C/22 discharge to 2.0 V | 10.5 °C | 2.393 | 2.556 | 2.598 |

At both extremes the voltage is still moving tens of mV per half hour after 1 h. These are the steep parts of the curve, so the movement is worth only about 1 SoC point. The practical point is that a pack resting near full still reads above its settled OCV after 2 h, and so maps a little *high*.

## 3. Hysteresis: how much is real

- In the zero-current limit, LFP keeps a **~20 mV charge/discharge hysteresis that does not relax away**: "non-vanishing ∼20 mV hysteresis in the zero-current limit (at C/1000) … demonstrated by Dreyer et al." It comes from the many-particle phase transition and is not a kinetic effect ([Malik, Abdellahi & Ceder, *J. Electrochem. Soc.* 160 (5) A3179, 2013](https://ceder.berkeley.edu/publications/2013_LFP_Review_paper.pdf), citing W. Dreyer et al., *Nat. Mater.* 9, 448, 2010). The voltage a rested pack shows therefore depends on whether it got there by charging or by discharging. That is **±~10 mV about the mid curve**.
- The CALCE C/22 half-gap (23–36 mV at 25 °C) is that ~10 mV plus the C/22 overpotential, so it is an upper bound.
- The 2025 GITT study of a commercial LFP cell ([Chen, *Electronics Letters*, doi:10.1049/ell2.70403](https://ietresearch.onlinelibrary.wiley.com/doi/full/10.1049/ell2.70403)) reports that "OCV has minor dependence on temperature but depends on SOC, while the hysteresis … depend[s] on both temperature and SOC" and that "hysteresis decreases as the temperature increases". This is from the abstract only; the full text returned 403.

**What ±10 mV does to the SoC reading** (slope of the 25 °C mid curve):

| Band | Slope | ±10 mV → |
|---|---|---|
| 1–8 % | 25–60 mV / point | < 0.5 point |
| 8–20 % | 3–5 mV / point (flattest at 9–14 %) | ±2–3 points |
| 80–92 % | **< 1 mV / point** | **±10 points or more** |
| 94–97 % | 3–12 mV / point | ±1–3 points |
| ≥ 97 % | > 20 mV / point | < 0.5 point |

## 4. Source disagreement at the ends

Only one fully measured dataset could be accessed: CALCE A123, two cells. The disagreement we can quantify:

- **Cell to cell (same model, same test):** ≤ 9 mV at 1–8 %, ≤ 4 mV at 10–30 % and 80–99 %. At the low end that is ≤ 1 point. At 85–92 % it is 2–4 mV, which is already 2–4 points.
- **Small cylindrical A123 vs large prismatic Hithium 280 Ah:** Hithium's shipping OCV of 3.28–3.30 V at 27 % SoC compares with 3.276 V (mid) and 3.31 V (charge branch) on the A123 at 27 %. That is a 0–25 mV gap, consistent with a cell charged up to its shipping SoC and resting on the charge branch. At the low end, 20 mV is about 5 points. At 85–92 % it would be more than 15 points.
- **SoC definition:** CALCE's 0 % is a C/22 discharge to 2.0 V. A Seplos BMS's 0 % sits at a higher cutoff (typically 2.5–2.8 V). Below 3 % that moves the scale by about 1–2 points, and above about 5 % it has no practical effect.
- **Not checked:** large-format LFP GITT tables from journals (paywalled), and the KTH 10 Ah prismatic thesis ([diva2:1791992](https://kth.diva-portal.org/smash/get/diva2:1791992/FULLTEXT01.pdf), download timed out).

**Conclusion:** sources disagree by roughly 5–25 mV. That is a few SoC points below 20 %, which the 10-point margin absorbs. In the 80–95 % band it is enough to swamp the margin.

## 5. Temperature, 10–25 °C

- On the pseudo-OCV mid curve, 10 °C vs 25 °C differs by −5 to −8 mV at 1–30 % SoC and +1 to +9 mV at 85–98 % (table 2.1). At 10–20 % that is ≤ 2 points. At 90–95 % it is about 3 points.
- The thermodynamic temperature coefficient of an LFP cell is small and "relatively independent of temperature" over most of the SoC range. It is strongly temperature-dependent only at 0 % and 100 % SoC ([Bazinski & Wang, *J. Electrochem. Soc.* 161 (1) A168, 2014](https://www.secs.oakland.edu/~wang/doc/publication/J.%20Electrochem.%20Soc.-2014-Bazinski-A168-75.pdf), 14 Ah LFP pouch, −20 to 55 °C).
- The larger cold effect is kinetic. The C/22 half-gap grows from ~32 to ~47 mV at 10 °C and to 60–120 mV at 0 °C near the ends, and relaxation is slower (table 2.2). Below 10 °C a 2 h rest leaves a discharge-rested pack reading noticeably low. At 0 °C the 20 % discharge-branch voltage (3.18 V) maps to about 8 % on the 25 °C table. **The 10 °C gate is justified.** No temperature correction of the table is needed above 10 °C.

## 6. Proposed default table

Per-cell rested OCV, 25 °C, mid curve (CALCE A123 pseudo-OCV, mean of A1-007/A1-008). Interpolate linearly. Clamp below the first point to ≤ 1 % and above the last to 100 %.

| V | SoC % |
|---|---|
| 2.790 | 1 |
| 2.915 | 2 |
| 2.995 | 3 |
| 3.100 | 5 |
| 3.165 | 7 |
| 3.193 | 8 |
| 3.209 | 10 |
| 3.215 | 12 |
| 3.226 | 15 |
| 3.237 | 17 |
| 3.251 | 20 |
| 3.269 | 25 |
| 3.283 | 30 |
| 3.303 | 40 |
| 3.307 | 50 |
| 3.310 | 60 |
| 3.325 | 70 |
| 3.346 | 80 |
| 3.352 | 90 |
| 3.361 | 94 |
| 3.366 | 95 |
| 3.374 | 96 |
| 3.386 | 97 |
| 3.410 | 98 |
| 3.455 | 99 |
| 3.490 | 100 |

The 30–90 % rows exist only to keep the table monotonic. The rule must never trust them: 3.30–3.35 V covers ~38–90 %.

## 7. Implications for the rule in #7

1. **Low end: keep it as specified.** OCV SoC < 20 %, a 10-point margin and a 2 h rest at ≥ 10 °C are consistent with the evidence. The worst realistic bias is a discharge-rested pack reading 2–4 points *low*. Optionally, tighten the trust band to < 15 %, where the margin is largest relative to the bias.
2. **High end: change it.** "> 90 %" falls in the flattest part of the curve. Trust it only at **OCV SoC ≥ 97 % (≥ 3.39 V/cell average)**, or drop the upper rested anchor and rely on the full-charge anchor, which already covers that state. A 90 % trust band with a 10-point margin will produce false drift findings after partial charges.
3. **Direction matters more than temperature.** If bms-flux knows whether the last significant current before rest was charge or discharge, it could shift the table by ∓10 mV. This is not needed for the low-end rule as specified.
4. **Cell average vs extremes.** On a 16S pack near the ends, the cells fan out (the steep regions amplify imbalance). The average is the right input for the table. A large max−min spread at rest should be carried as evidence in the finding.

## Sources

- CALCE Battery Data, A123 low-current OCV tests — https://calce.umd.edu/battery-data (raw files `A123_OCV{0,10,20,25}-*.zip`, analysed directly)
- R. Malik, A. Abdellahi, G. Ceder, "A Critical Review of the Li Insertion Mechanisms in LiFePO4 Electrodes", *J. Electrochem. Soc.* 160 (5) A3179–A3197 (2013) — https://ceder.berkeley.edu/publications/2013_LFP_Review_paper.pdf
- W. Dreyer et al., "The thermodynamic origin of hysteresis in insertion batteries", *Nat. Mater.* 9, 448 (2010) — cited via Malik et al.
- S. J. Bazinski, X. Wang, "The Influence of Cell Temperature on the Entropic Coefficient of a Lithium Iron Phosphate (LFP) Pouch Cell", *J. Electrochem. Soc.* 161 (1) A168–A175 (2014), doi:10.1149/2.082401jes
- Chen, "Dependence of OCV, Hysteresis, Overvoltage on Temperature and SOC in Commercial LiFePO4 Lithium-Ion Battery", *Electronics Letters* (2025), doi:10.1049/ell2.70403 (abstract only)
- EVE LF280K / LF280N / LF280 / LF304 / MB31 and Hithium 280 Ah product specifications (links in section 1)
