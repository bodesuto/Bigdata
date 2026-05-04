from __future__ import annotations

from dataclasses import dataclass

from .config import PipelineConfig
from .features import receiver_balance_inconsistent, sender_balance_inconsistent
from .models import FraudDecision, TransactionEvent


HIGH_RISK_TYPES = {"TRANSFER", "CASH_OUT"}


@dataclass
class RuleEngine:
    config: PipelineConfig

    def evaluate(
        self,
        event: TransactionEvent,
        recent_sender_events: list[TransactionEvent] | None = None,
        watchlisted_accounts: set[str] | None = None,
    ) -> FraudDecision:
        recent_sender_events = recent_sender_events or []
        watchlisted_accounts = watchlisted_accounts or set()
        triggered_rules: list[str] = []
        rule_risk_score = 0.0

        if self._is_high_amount_transfer(event):
            triggered_rules.append("high_amount_transfer")
            rule_risk_score += 0.45

        if sender_balance_inconsistent(event, self.config):
            triggered_rules.append("sender_balance_inconsistency")
            rule_risk_score += 0.30

        if receiver_balance_inconsistent(event, self.config):
            triggered_rules.append("receiver_balance_inconsistency")
            rule_risk_score += 0.15

        if event.is_flagged_fraud:
            triggered_rules.append("flagged_fraud")
            rule_risk_score += 0.10

        if self._has_rapid_outflow(event, recent_sender_events):
            triggered_rules.append("rapid_outflow_pattern")
            rule_risk_score += 0.25

        if event.name_orig in watchlisted_accounts or event.name_dest in watchlisted_accounts:
            triggered_rules.append("watchlist_hit")
            rule_risk_score += 0.35

        # Hybrid: Integrate ML Score
        ml_score = self._predict_ml_score(event)
        
        # Combined score: 60% Rules, 40% ML
        combined_risk_score = min((rule_risk_score * 0.6) + (ml_score * 0.4), 1.0)
        
        is_alert = bool(triggered_rules) or ml_score >= 0.85
        severity = "high" if combined_risk_score >= 0.75 else "medium" if combined_risk_score >= 0.40 else "low"
        
        return FraudDecision(
            event_id=event.event_id,
            is_alert=is_alert,
            risk_score=round(combined_risk_score, 4),
            severity=severity,
            ml_score=ml_score,
            ml_model_version="v1_paysim_rf",
            triggered_rules=tuple(triggered_rules),
        )

    def _predict_ml_score(self, event: TransactionEvent) -> float:
        """
        Simulated ML model inference (e.g. Random Forest or XGBoost).
        In production, this would call a pre-loaded model.
        """
        # Feature: If amount is very large and it's a TRANSFER, increase ML risk
        if event.txn_type == "TRANSFER" and event.amount > 500000:
            return 0.88
        # Feature: If it's a CASH_OUT and balance becomes 0
        if event.txn_type == "CASH_OUT" and event.newbalance_orig == 0:
            return 0.75
        return 0.12

    def _is_high_amount_transfer(self, event: TransactionEvent) -> bool:
        if event.txn_type == "TRANSFER":
            return event.amount >= self.config.high_amount_transfer_threshold
        if event.txn_type == "CASH_OUT":
            return event.amount >= self.config.high_amount_cash_out_threshold
        return False

    def _has_rapid_outflow(self, event: TransactionEvent, recent_sender_events: list[TransactionEvent]) -> bool:
        if not recent_sender_events:
            return False
        window_start = event.event_time.timestamp() - self.config.rapid_outflow_window_seconds
        matching = [
            item
            for item in recent_sender_events
            if item.name_orig == event.name_orig and item.event_time.timestamp() >= window_start
        ]
        total_amount = sum(item.amount for item in matching) + event.amount
        return (
            len(matching) + 1 >= self.config.rapid_outflow_count_threshold
            or total_amount >= self.config.rapid_outflow_amount_threshold
        )
