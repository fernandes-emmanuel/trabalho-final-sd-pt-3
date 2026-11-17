import json

import pytest

from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from mpcomm.ordering.total_order import Ordering
from mpcomm.transport.faults import FaultProfile

pytestmark = pytest.mark.integration


def deploy(tmp_path, ordering, base_port):
    report_path = tmp_path / f"{ordering}.json"
    deployment = LocalDeployment(
        offices=4,
        sessions=2,
        workload=WorkloadSpec(commands=25, seed=1),
        ordering=ordering,
        base_port=base_port,
        faults=FaultProfile(loss=0.1, delay_ms=15, jitter_ms=15, seed=5),
        report=report_path,
        timeout=300,
    )
    exit_code = run_local(deployment)
    return exit_code, json.loads(report_path.read_text())


def test_total_order_keeps_every_replica_identical_under_loss_and_jitter(tmp_path):
    exit_code, reports = deploy(tmp_path, Ordering.TOTAL, 7600)

    assert exit_code == 0
    assert len(reports) == 2
    for report in reports:
        assert report["ordering"] == "total"
        assert report["complete"]
        assert report["consistent"]
        assert report["first_divergent_position"] is None
        assert all(office["applied"] == 100 for office in report["offices"])


def test_fifo_order_still_delivers_every_command(tmp_path):
    exit_code, reports = deploy(tmp_path, Ordering.FIFO, 7650)

    assert exit_code == 0
    assert all(report["complete"] and report["ordering"] == "fifo" for report in reports)
