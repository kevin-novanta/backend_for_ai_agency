"""
Parallel email dispatcher
-------------------------
Runs one worker per inbox so multiple inboxes can send in parallel—just like tools
such as Woodpecker. Each worker sends to different leads with jittered delays so
sends are spaced naturally and within per‑inbox daily caps.

This module is *pure orchestration*: it doesn't generate copy, verify emails,
or write CSVs. You plug in callbacks from `sequence_runner.py` for those steps.

Key ideas
- One asyncio worker per inbox (parallel across inboxes).
- Jitter between sends per inbox (default 60–120s) to mimic human pacing.
- Respect per‑inbox and optional global daily limits.
- Route each lead to exactly one inbox via a provided `choose_inbox_cb`.
- Call your provided `send_one_cb` to actually generate/personalize/send.
- Optional `on_result_cb` lets you update the CRM/CSV as soon as a send completes.

Integrate from sequence_runner.py (example):

    from workflows.outreach_sender.Utils.parallel_dispatcher import run_parallel_dispatch

    results = run_parallel_dispatch(
        leads=eligible_leads,
        sender_pool=sender_pool_emails,  # list[str]
        send_one_cb=send_one_opener,     # (inbox_email:str, lead:dict) -> dict result
        choose_inbox_cb=resolve_inbox_for_lead, # (lead:dict, sender_pool:list[str]) -> str inbox
        on_result_cb=on_send_result,     # optional (lead, inbox, result_dict) -> None
        jitter_seconds=(60, 120),
        per_inbox_daily_limit=controls.get("daily_limit_per_inbox", 200),
        global_daily_limit=controls.get("daily_limit_total"),
        max_inboxes=None,  # or an int to cap number of concurrently active inboxes
    )

Where `send_one_opener` should raise an Exception on failure or return a dict like:
    {
      "ok": True,
      "subject": str,
      "body_html": str,
      "bounce_status": "none" | "hard" | "soft" | "blocked",
      "timestamp": iso8601_str,
      "sender_used": inbox_email,
    }

This file uses only stdlib asyncio/random/time and prints lightweight [DISPATCH] logs.
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# Type aliases for clarity
Lead = Dict[str, Any]
SendResult = Dict[str, Any]

ChooseInboxCB = Callable[[Lead, List[str]], str]
SendOneCB = Callable[[str, Lead], SendResult]
OnResultCB = Callable[[Lead, str, SendResult], None]


class ParallelDispatcher:
    def __init__(
        self,
        sender_pool: List[str],
        *,
        jitter_seconds: Tuple[int, int] = (60, 120),
        per_inbox_daily_limit: int = 200,
        global_daily_limit: Optional[int] = None,
        max_inboxes: Optional[int] = None,
    ) -> None:
        if not sender_pool:
            raise ValueError("sender_pool must contain at least one inbox email")
        self.sender_pool = list(sender_pool)
        self.min_jitter, self.max_jitter = jitter_seconds
        if self.min_jitter <= 0 or self.max_jitter < self.min_jitter:
            raise ValueError("jitter_seconds must be a tuple like (60,120)")
        self.per_inbox_daily_limit = int(per_inbox_daily_limit)
        self.global_daily_limit = int(global_daily_limit) if global_daily_limit else None
        self.max_inboxes = int(max_inboxes) if max_inboxes else None

        # Shared state across workers
        self._global_sent = 0
        self._global_lock = asyncio.Lock()
        self._stop = asyncio.Event()

    async def _worker(
        self,
        inbox: str,
        queue: "asyncio.Queue[Lead]",
        send_one_cb: SendOneCB,
        on_result_cb: Optional[OnResultCB],
    ) -> None:
        sent_count = 0
        while not self._stop.is_set():
            print(f"[DISPATCH] Worker for inbox {inbox} started processing loop.")
            try:
                lead = await asyncio.wait_for(queue.get(), timeout=0.5)
                print(f"[DISPATCH] Inbox {inbox}: fetched lead with Email={lead.get('Email')}")
            except asyncio.TimeoutError:
                # No more leads currently queued for this inbox
                if queue.empty():
                    print(f"[DISPATCH] Inbox {inbox}: queue empty, ending worker loop.")
                    break
                continue

            print(f"[DISPATCH] Inbox {inbox}: checking per-inbox daily limit ({sent_count}/{self.per_inbox_daily_limit})")
            # Check per-inbox cap
            if sent_count >= self.per_inbox_daily_limit:
                print(f"[DISPATCH] Inbox {inbox}: daily limit reached, skipping remaining queued leads ({queue.qsize()}).")
                queue.task_done()
                continue

            print(f"[DISPATCH] Inbox {inbox}: checking global daily limit.")
            # Check global cap
            async with self._global_lock:
                if self.global_daily_limit is not None and self._global_sent >= self.global_daily_limit:
                    print("[DISPATCH] Global daily limit reached. Stopping all workers.")
                    self._stop.set()
                    queue.task_done()
                    break

            # Jitter *before* send for natural spacing per inbox
            delay = random.uniform(self.min_jitter, self.max_jitter)
            print(f"[DISPATCH] Inbox {inbox}: calculated delay {delay:.1f}s, sleeping before next send.")
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                queue.task_done()
                break

            # Perform the send
            print(f"[DISPATCH] Inbox {inbox}: about to send to lead Email={lead.get('Email')}")
            started = time.time()
            try:
                result = send_one_cb(inbox, lead)
                print(f"[DISPATCH] Inbox {inbox}: raw send_one_cb result: {result}")
                ok = bool(result.get("ok", True))
            except Exception as e:  # noqa: BLE001 — we want to keep sending other leads
                ok = False
                result = {"ok": False, "error": str(e)}
                print(f"[DISPATCH] Inbox {inbox}: send_one_cb raised exception: {e}")

            elapsed = time.time() - started
            status = "OK" if ok else "FAIL"
            print(f"[DISPATCH] Inbox {inbox}: send {status} in {elapsed:.2f}s → {lead.get('Email')}")

            if ok:
                # Update counts only on success
                sent_count += 1
                async with self._global_lock:
                    self._global_sent += 1

            if on_result_cb:
                print(f"[DISPATCH] Inbox {inbox}: invoking on_result_cb for lead Email={lead.get('Email')}")
                try:
                    on_result_cb(lead, inbox, result)
                except Exception as e:  # noqa: BLE001
                    print(f"[DISPATCH] on_result_cb error for {lead.get('Email')}: {e}")

            queue.task_done()

    async def dispatch_async(
        self,
        *,
        leads: Iterable[Lead],
        choose_inbox_cb: ChooseInboxCB,
        send_one_cb: SendOneCB,
        on_result_cb: Optional[OnResultCB] = None,
    ) -> List[SendResult]:
        """Route leads to per‑inbox queues and run workers in parallel.

        Returns a list of result dicts (only successes if your callback is written
        that way). You can also persist inside `on_result_cb` to stream results out.
        """
        print(f"[DISPATCH] dispatch_async started with {len(list(leads))} leads.")
        # Build per‑inbox queues
        active_senders = self.sender_pool[: self.max_inboxes] if self.max_inboxes else self.sender_pool
        queues: Dict[str, asyncio.Queue] = {s: asyncio.Queue() for s in active_senders}

        # Route each lead to an inbox (callback decides; we fallback to round‑robin)
        rr_index = 0
        routed_count = 0
        # Re-create leads iterable to list since we consumed it above
        leads_list = list(leads)
        for lead in leads_list:
            try:
                inbox = choose_inbox_cb(lead, active_senders)
            except Exception:
                inbox = active_senders[rr_index % len(active_senders)]
                rr_index += 1
            queues[inbox].put_nowait(lead)
            routed_count += 1
        print(f"[DISPATCH] Routed {routed_count} leads across {len(active_senders)} inboxes.")
        for inbox, q in queues.items():
            print(f"[DISPATCH] Inbox {inbox} queued leads: {q.qsize()}")

        # Spin up workers (one per inbox) and run until all queues are drained
        inboxes_to_run = [inbox for inbox, q in queues.items() if not q.empty()]
        print(f"[DISPATCH] Starting workers for inboxes: {inboxes_to_run}")
        tasks = [
            asyncio.create_task(self._worker(inbox, q, send_one_cb, on_result_cb))
            for inbox, q in queues.items()
            if not q.empty()
        ]
        if not tasks:
            print("[DISPATCH] No work to do (all queues empty).")
            return []

        try:
            await asyncio.gather(*tasks)
            print("[DISPATCH] All worker tasks completed.")
        finally:
            self._stop.set()

        print(f"[DISPATCH] Completed. Global sent: {self._global_sent}")
        # This function streams results via callback; return value is mostly for symmetry
        return []


def run_parallel_dispatch(
    *,
    leads: Iterable[Lead],
    sender_pool: List[str],
    send_one_cb: SendOneCB,
    choose_inbox_cb: ChooseInboxCB,
    on_result_cb: Optional[OnResultCB] = None,
    jitter_seconds: Tuple[int, int] = (60, 120),
    per_inbox_daily_limit: int = 200,
    global_daily_limit: Optional[int] = None,
    max_inboxes: Optional[int] = None,
) -> List[SendResult]:
    """Synchronous entrypoint for sequence_runner.

    This wraps the async dispatcher with `asyncio.run`, so callers don't need to
    manage an event loop.
    """
    print(f"[DISPATCH] run_parallel_dispatch invoked with {len(list(leads))} leads and {len(sender_pool)} sender_pool inboxes.")
    dispatcher = ParallelDispatcher(
        sender_pool,
        jitter_seconds=jitter_seconds,
        per_inbox_daily_limit=per_inbox_daily_limit,
        global_daily_limit=global_daily_limit,
        max_inboxes=max_inboxes,
    )
    results = asyncio.run(
        dispatcher.dispatch_async(
            leads=leads,
            choose_inbox_cb=choose_inbox_cb,
            send_one_cb=send_one_cb,
            on_result_cb=on_result_cb,
        )
    )
    print("[DISPATCH] run_parallel_dispatch finished.")
    return results
