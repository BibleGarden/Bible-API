# Runpod Gemma 4 deployment result

## Resource record

- Pod ID: `8q0uiy9j7lon07`
- Name: `bible-gemma4-31b-fp8-86cbh0p8t`
- Created: `2026-09-12T07:29:55.668Z` (`2026-09-12 10:29:55` Europe/Moscow)
- Mandatory termination deadline: before `2026-09-12T09:29:55.668Z` (`12:29:55` Europe/Moscow)
- One Secure Cloud `NVIDIA L40S`, requested in `EUR-IS-2`
- Live compute quote immediately before creation: `$1.09/hour`
- Storage: 30 GB container disk plus 60 GB persistent Pod volume. Estimated local-disk charge from the published `$0.10/GB/month` running rate: about `$0.0125/hour`; verify the final account charge after termination.
- Expected maximum for two hours at these rates: about `$2.21`.
- Initial API state returned by create-pod: `RUNNING`. This is container state, not model readiness.
- The pinned Linux amd64 image contains 34 compressed layers totaling 9,203,858,315 bytes (8.572 GiB), measured from the Docker Registry manifest. The first cold pull was still active 22 minutes after Pod creation; at `07:52:10Z` three large layers remained in the system log. Model-weight download had not started yet.

The model API credential was supplied through `VLLM_API_KEY` and is intentionally absent from this artifact. See `deployment-plan.md` and `preflight.md` for the pinned image, model revision, and launch arguments.

## Verification

Pending container logs, authenticated external model-list response, and authenticated generation response.
