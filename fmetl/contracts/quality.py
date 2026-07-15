from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class QualityReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(CheckResult(name=name, passed=passed, detail=detail))

    def raise_if_failed(self) -> None:
        failures = [check for check in self.checks if not check.passed]
        if failures:
            message = "; ".join(f"{item.name}: {item.detail}" for item in failures)
            raise RuntimeError(message)
