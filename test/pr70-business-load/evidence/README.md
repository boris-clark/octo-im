# Preserved evidence

These files are kept byte-for-byte, so the history of this harness can be audited from the tree
alone. Newer evidence for the current head is delivered as platform attachments on XIN-2200,
because a commit cannot contain its own receipts.

## `xin2200-delivery-6255599a/`: first delivery, head `6255599a3de8602b250f684826b0df19edc7b946`

| File | SHA-256 | Source |
|---|---|---|
| `XIN-2200-harness-evidence.md` | `953400a49165e20ee96f58817a1f4751e9188b9b4676320363dad5fc7a86392c` | XIN-2200 comment `02ebc099`, attachment `01a13e87-f2db-70ea-8870-0b0dfffe970f` |
| `selftest-receipt.txt` | `917952868f9f2ef67ab3708190a1fb8cfdd4224e47d523f993085f8cd0894677` | same comment, attachment `01a13e87-f361-74ce-8357-9e02f5c3b1b2` |
| `SHA256SUMS` | (lists the four delivery hashes) | same comment, attachment `01a13e87-f373-7f26-ab8f-72cb18632a53` |

The two binary delivery artifacts are not copied here. They remain attached to the same comment and
are verifiable by `SHA256SUMS`:
- `pr70-business-load-harness.bundle`, `10f0e23f…aaafb`, attachment `01a13e87-f325-7365-ad41-eb1f77db050f`;
- `pr70-business-load-harness-6255599a.tar.gz`, `109ef7b1…c667c`, attachment `01a13e87-f347-7a70-8ea2-d4126c91916f`.

That delivery was reviewed CHANGES_REQUESTED, and its green receipts did not cover blockers B1/B2 below.

## `xin2204-review-6255599a/`: independent review of `6255599a`, verdict CHANGES_REQUESTED

| File | SHA-256 | Source |
|---|---|---|
| `XIN-2204-review-report.md` | `f4c7f814f4164193c9261254fdf5f7dd37e795a157aa1cc2fef8597f67672e83` | XIN-2204 comment `2501e9c2`, attachment `01a13e94-088e-71da-a6a2-e2a1ead74d07` |
| `controls.py` | `e256417e838d33da9270733d36e1cc07fe1e0274b941e259a7e26a63fdcf609e` | attachment `01a13e94-08b8-7d54-9d44-cdeec791612e` |
| `controls-result.json` | `6d0f6d87e1f1a651d6b0a67388ec0ea3c504d053d0310a32e4dac4135ed8b808` | attachment `01a13e94-0906-763b-8583-f84900d55641` |

The reviewer's self-test log (`selftest-reviewer.out`, `6983467d94f2884230e006c8fe5d8427421c2a093baabfe907d25e4e2f1cff1e`,
attachment `01a13e94-0931-77d8-9edf-b7a6d03bd42c`) stays on the platform only. It embeds the
reviewer's local temporary paths.

`controls-result.json` shows the failed controls on `6255599a`. X0 (diverged node omitted from
`--manager`), X1 (diverged node unreachable) and X2 (confirmed uid, rejected add, uid lost) all
returned PASS. The corrected head ships equivalent tests in `tests/test_harness.py`
(`TestB1*`, `TestB2*`). `controls.py` is unchanged, and re-running it against the correction is
part of the current delivery evidence.
