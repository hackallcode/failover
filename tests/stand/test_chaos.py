"""Random faults in a loop, then the invariants over the whole run and recovery after the last heal."""
import os
import random
import time

from tests.stand import stand
from tests.stand.test_scenarios import FENCE, RECOVERY, SYNC, check_single_writer

STEPS = int(os.environ.get("CHAOS_STEPS", "20"))
SEED = int(os.environ.get("CHAOS_SEED", str(int(time.time()))))


def faults(rng):
    node = rng.choice(("a", "b", "c"))
    agent = rng.choice(stand.NODES)
    return rng.choice([
        (f"kill {node}", lambda: stand.kill(node)),
        (f"isolate {node}", lambda: stand.isolate(node, True)),
        (f"pause agent {agent}", lambda: stand.pause(agent, True)),
        (f"switch to {agent}", lambda: stand.request(agent, check=False)),
    ])


def test_chaos(cluster):
    rng = random.Random(SEED)
    print(f"CHAOS_SEED={SEED}")
    for step in range(STEPS):
        for _ in range(rng.randint(1, 2)):
            name, fault = faults(rng)
            print(f"step {step}: {name}")
            fault()
        time.sleep(rng.uniform(2, 25))
        if rng.random() < 0.6:
            stand.heal()
            time.sleep(rng.uniform(0, 15))
    stand.heal()
    leader = stand.wait_serving(2 * RECOVERY)
    assert leader, f"no leader after the last heal (CHAOS_SEED={SEED})"
    time.sleep(SYNC)
    rows = stand.writes()
    check_single_writer(rows)
    # isolation is the worst case: the isolated leader commits until it fences
    lost = stand.unexpected_losses(rows, leader, FENCE + SYNC)
    assert not lost, f"{len(lost)} committed rows lost, first {lost[0]} (CHAOS_SEED={SEED})"
