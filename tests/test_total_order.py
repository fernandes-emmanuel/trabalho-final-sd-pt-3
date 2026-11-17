import random
from collections import defaultdict

import pytest

from mpcomm.ordering.total_order import LamportClock, Stamp, TotalOrderMulticast
from mpcomm.transport.messages import Kind
from mpcomm.transport.reliable import ReliableFifoMulticast
from tests.simulation import SimulatedNetwork

SESSION = 1


def build_group(network, size, delivered, *, total_order=True):
    members = {member: ("node", member) for member in range(size)}
    stacks = {}
    for member in members:
        reliable = ReliableFifoMulticast(
            round_id=SESSION,
            self_id=member,
            members=members,
            channel=network,
            deliver=lambda packet, member=member: stacks[member][2](packet.sender, packet.payload),
            clock=network.clock,
        )
        record = delivered[member].append
        if total_order:
            total = TotalOrderMulticast(
                self_id=member,
                members=members,
                transmit=lambda payload, reliable=reliable: reliable.multicast(Kind.DATA, payload),
                deliver=lambda sender, body, record=record: record((sender, body)),
            )
            stacks[member] = (reliable, total, total.receive)
        else:
            stacks[member] = (
                reliable,
                None,
                lambda sender, body, record=record: record((sender, body)),
            )
        network.attach(members[member], reliable.handle)
        network.every(0.01, reliable.retransmit_due)
    return stacks


def schedule_traffic(network, stacks, count, seed):
    rng = random.Random(seed)
    for member, (reliable, total, _) in stacks.items():
        moments = sorted(rng.uniform(0, 0.5) for _ in range(count))
        for number, moment in enumerate(moments):
            body = {"from": member, "n": number}
            if total is not None:
                network.at(moment, lambda total=total, body=body: total.multicast(body))
            else:
                network.at(
                    moment,
                    lambda reliable=reliable, body=body: reliable.multicast(Kind.DATA, body),
                )
        if total is not None:
            network.at(0.6, total.announce_end)


@pytest.mark.parametrize("seed", range(25))
def test_every_member_delivers_the_same_sequence_despite_loss_and_reordering(seed):
    network = SimulatedNetwork(seed, loss=0.2, duplication=0.05, min_delay=0.001, max_delay=0.1)
    delivered = defaultdict(list)
    size, count = 4, 15
    stacks = build_group(network, size, delivered)
    schedule_traffic(network, stacks, count, seed)

    def everyone_finished():
        return all(total.wait_until_everyone_ended(0) for _, total, _ in stacks.values())

    assert network.run_until(everyone_finished)
    sequences = [delivered[member] for member in stacks]
    assert all(sequence == sequences[0] for sequence in sequences)
    assert len(sequences[0]) == size * count
    for sender in stacks:
        numbers = [body["n"] for origin, body in sequences[0] if origin == sender]
        assert numbers == list(range(count))


def test_fifo_alone_does_not_guarantee_a_common_order():
    diverged = 0
    for seed in range(10):
        network = SimulatedNetwork(seed, min_delay=0.001, max_delay=0.1)
        delivered = defaultdict(list)
        stacks = build_group(network, 4, delivered, total_order=False)
        schedule_traffic(network, stacks, 15, seed)
        network.run_until(lambda d=delivered, s=stacks: all(len(d[m]) == 60 for m in s))
        sequences = [delivered[member] for member in stacks]
        diverged += any(sequence != sequences[0] for sequence in sequences)
    assert diverged > 0


class Recorder:
    def __init__(self):
        self.sent = []
        self.delivered = []


def make_member(self_id, members=(0, 1)):
    recorder = Recorder()
    member = TotalOrderMulticast(
        self_id=self_id,
        members=members,
        transmit=recorder.sent.append,
        deliver=lambda sender, body: recorder.delivered.append((sender, body)),
    )
    return member, recorder


def test_message_waits_for_acknowledgements_from_every_member():
    member, recorder = make_member(0)

    member.receive(1, {"order": "msg", "ts": 1, "body": "x"})
    assert recorder.delivered == []
    assert recorder.sent[-1]["order"] == "ack"
    assert recorder.sent[-1]["ref"] == [1, 1]

    member.receive(0, {"order": "ack", "ts": 3, "ref": [1, 1]})
    assert recorder.delivered == []

    member.receive(1, {"order": "ack", "ts": 2, "ref": [1, 1]})
    assert recorder.delivered == [(1, "x")]


def test_smaller_timestamp_is_delivered_first_and_ties_break_by_sender():
    member, recorder = make_member(0)
    member.receive(1, {"order": "msg", "ts": 5, "body": "late"})
    member.receive(0, {"order": "msg", "ts": 5, "body": "tie"})
    member.receive(1, {"order": "msg", "ts": 2, "body": "early"})
    for stamp in ([5, 1], [5, 0], [2, 1]):
        for acker in (0, 1):
            member.receive(acker, {"order": "ack", "ts": 9, "ref": stamp})

    assert [body for _, body in recorder.delivered] == ["early", "tie", "late"]


def test_head_of_queue_blocks_later_messages_until_acknowledged():
    member, recorder = make_member(0)
    member.receive(1, {"order": "msg", "ts": 1, "body": "first"})
    member.receive(1, {"order": "msg", "ts": 2, "body": "second"})
    for acker in (0, 1):
        member.receive(acker, {"order": "ack", "ts": 9, "ref": [2, 1]})
    assert recorder.delivered == []

    for acker in (0, 1):
        member.receive(acker, {"order": "ack", "ts": 9, "ref": [1, 1]})
    assert [body for _, body in recorder.delivered] == ["first", "second"]


def test_end_markers_are_ordered_but_not_delivered_to_the_application():
    member, recorder = make_member(0)
    member.announce_end()
    member.receive(0, {"order": "end", "ts": 1})
    member.receive(1, {"order": "end", "ts": 1})
    for stamp in ([1, 0], [1, 1]):
        for acker in (0, 1):
            member.receive(acker, {"order": "ack", "ts": 4, "ref": stamp})

    assert member.wait_until_everyone_ended(0)
    assert recorder.delivered == []


def test_malformed_messages_are_ignored():
    member, recorder = make_member(0)
    member.receive(1, {"order": "msg"})
    member.receive(1, {"order": "ack", "ts": 1, "ref": "bogus"})
    assert recorder.delivered == []


def test_lamport_clock_rules():
    clock = LamportClock()
    assert clock.tick() == 1
    clock.observe(10)
    assert clock.time == 11
    clock.observe(3)
    assert clock.time == 12
    assert Stamp(3, 1) < Stamp(4, 0) < Stamp(4, 2)
